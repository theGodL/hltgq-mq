package com.qgyun.hltgq.hltgqmq.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * MQTT 闸门数据 Redis 持久化缓冲。
 *
 * <p>报文到达即 {@code RPUSH} 写入每站一个 List（进程重启 / DB 短暂不可用都不丢），
 * 定时任务 {@code LRANGE} 取出落库，成功后 {@code LTRIM} 删除已落库条目。
 * 只使用单 key 命令，Cluster 下无 CROSSSLOT 风险；key 带 hash tag {@code {siteId}} 便于后续扩展。
 *
 * <p>可靠落库流程（至少一次）：
 * <ol>
 *   <li>{@link #save} 追加写入队列；</li>
 *   <li>{@link #fetch} 读取队首一批（不删除）；</li>
 *   <li>调用方落库成功后 {@link #ack} 按条数 LTRIM 删除；失败则不删，下轮重试，
 *       由闸门表 site+device+gate_no+tm 去重兜底重复。</li>
 * </ol>
 * RPUSH 追加在队尾、LTRIM 从队首删除，因此 fetch 与 ack 之间新到的报文不会被误删。
 *
 * <p>Redis 不可用时 {@link #save} 返回 false，调用方降级到内存缓存，不影响主流程。
 */
@Component
public class MqttRedisBuffer {

    private static final Logger log = LoggerFactory.getLogger(MqttRedisBuffer.class);

    private static final String QUEUE_PREFIX    = "mqtt:gate:queue:{%s}";
    private static final String ACTIVE_SITES_KEY = "mqtt:gate:activeSites";

    /** 队列 key 过期时间(秒)：防止长期无人消费的脏数据无限堆积 */
    private static final long TTL_SECONDS = 24 * 3600L;

    /** 单次最多取出的条数，避免一次拉爆内存 */
    private static final int MAX_FETCH = 1000;

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Redis 可用标志：一次失败后置 false，由下次成功调用自动恢复 */
    private volatile boolean available = true;

    public MqttRedisBuffer(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public boolean isAvailable() {
        return available;
    }

    /** 追加一条闸孔数据。成功返回 true；失败置不可用并返回 false（调用方走内存兜底）。 */
    public boolean save(String siteId, Map<String, Object> fieldMap) {
        try {
            String value = objectMapper.writeValueAsString(fieldMap);
            String key = queueKey(siteId);
            redis.opsForList().rightPush(key, value);
            redis.expire(key, TTL_SECONDS, TimeUnit.SECONDS);
            redis.opsForSet().add(ACTIVE_SITES_KEY, siteId);
            available = true;
            return true;
        } catch (Exception e) {
            if (available) {
                log.warn("MQTT Redis 缓冲写入失败, 降级内存缓存: site={}, {}", siteId, e.getMessage());
            }
            available = false;
            return false;
        }
    }

    /** 有缓冲数据的站点集合（单 key SET，非跨 slot 操作） */
    public Set<String> activeSites() {
        try {
            Set<String> sites = redis.opsForSet().members(ACTIVE_SITES_KEY);
            available = true;
            return sites != null ? sites : Collections.<String>emptySet();
        } catch (Exception e) {
            available = false;
            return Collections.emptySet();
        }
    }

    /**
     * 读取该站队首一批数据（不删除）。返回的 {@link Batch#count} 为实际读到的队列条目数，
     * 落库成功后按此数量调用 {@link #ack(String, int)}。
     */
    public Batch fetch(String siteId) {
        try {
            List<String> values = redis.opsForList().range(queueKey(siteId), 0, MAX_FETCH - 1);
            if (values == null || values.isEmpty()) {
                return new Batch(siteId, 0, Collections.<Map<String, Object>>emptyList());
            }
            return new Batch(siteId, values.size(), decode(values));
        } catch (Exception e) {
            log.warn("MQTT Redis 缓冲读取失败, 本批跳过: site={}, {}", siteId, e.getMessage());
            return new Batch(siteId, 0, Collections.<Map<String, Object>>emptyList());
        }
    }

    /** 落库成功后删除已消费的队首 count 条 */
    public void ack(String siteId, int count) {
        if (count <= 0) {
            return;
        }
        try {
            redis.opsForList().trim(queueKey(siteId), count, -1);
        } catch (Exception e) {
            log.warn("MQTT Redis 缓冲确认删除失败(下轮重试, 去重兜底): site={}, count={}, {}",
                    siteId, count, e.getMessage());
        }
    }

    private String queueKey(String siteId) {
        return String.format(QUEUE_PREFIX, siteId);
    }

    private List<Map<String, Object>> decode(List<String> values) {
        List<Map<String, Object>> rows = new ArrayList<>(values.size());
        for (String value : values) {
            try {
                Map<String, Object> row = objectMapper.readValue(
                        value, new TypeReference<LinkedHashMap<String, Object>>() {});
                rows.add(row);
            } catch (Exception ex) {
                log.warn("MQTT Redis 缓冲条目解析失败, 跳过: {}", ex.getMessage());
            }
        }
        return rows;
    }

    /** 一批待落库数据：siteId + 队列条目数 + 解析后的行 */
    public static final class Batch {
        public final String siteId;
        public final int count;
        public final List<Map<String, Object>> rows;

        Batch(String siteId, int count, List<Map<String, Object>> rows) {
            this.siteId = siteId;
            this.count = count;
            this.rows = rows;
        }

        public boolean isEmpty() {
            return count <= 0;
        }
    }
}
