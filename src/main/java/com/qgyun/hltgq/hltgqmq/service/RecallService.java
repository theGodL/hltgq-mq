package com.qgyun.hltgq.hltgqmq.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 召测服务：向 RabbitMQ 召测队列发送 remotedata 指令报文，
 * 上游报文服务消费后向现场 RTU 下发召测指令（37H 最新实时数据 / 38H 时段数据），
 * RTU 应答后上游会把最新报文加报一次到 MonitorData 队列，走既有消费入库链路。
 * <p>
 * 召测为同步等待模式：展示层调用后阻塞等待 RTU 应答加报入库（超时可配），
 * 返回 code=0 即代表数据已入库，前端可直接刷新。
 */
@Service
public class RecallService {

    private static final Logger log = LoggerFactory.getLogger(RecallService.class);

    /** 支持召测的四站白名单：stcd → 站名（其余站点不允许召测） */
    private static final Map<String, String> RECALL_SITE_MAP = new LinkedHashMap<>();
    static {
        RECALL_SITE_MAP.put("9000000001", "北干渠进水闸");
        RECALL_SITE_MAP.put("9000000002", "南干渠进水闸");
        RECALL_SITE_MAP.put("9000000005", "毕岭节制闸");
        RECALL_SITE_MAP.put("9000000006", "汪元节制闸");
    }

    /** 待确认召测：stcd → 指令发送时间(ms)，供 MonitorDataService 用 ctime 判据识别召测应答 */
    private final Map<String, Long> pendingRecall = new ConcurrentHashMap<>();

    /** 同步等待中的召测：stcd → 应答信号，MonitorDataService 确认应答后 complete */
    private final Map<String, CompletableFuture<Void>> waiters = new ConcurrentHashMap<>();

    /** 召测成功记录：stcd → 成功时刻(ms)，供展示层刷新页面后恢复"数据确认"按钮状态 */
    private final Map<String, Long> lastSuccess = new ConcurrentHashMap<>();

    /** 同步等待超时（秒）：展示层调用后最多等这么久，超时返回"召测无响应"。设备方反馈 RTU 应答最长 5 分钟，设 360 秒（5 分钟+1 分钟余量） */
    @Value("${recall.timeout-seconds:360}")
    private long timeoutSeconds;

    /** 应答到达后到接口返回的入库缓冲时间（毫秒）：让同批次剩余 tag 报文入库完成 */
    private static final long RESPONSE_SETTLE_MS = 2000L;

    /** 兜底清理窗口（毫秒）：防止 pendingRecall 泄漏（同步等待失败等异常路径） */
    private static final long RECALL_CLEANUP_WINDOW_MS = 10 * 60 * 1000L;

    /** "数据确认"状态保留窗口（毫秒）：成功后 5 分钟内刷新页面可恢复绿色按钮，超时自动复位 */
    private static final long CONFIRMED_KEEP_WINDOW_MS = 5 * 60 * 1000L;

    /** 召测指令队列（设备方提供，后续若变更仅改配置） */
    @Value("${recall.queue:rtu_ythzm_test}")
    private String recallQueue;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private ObjectMapper objectMapper;

    /** 指令报文时间格式，与设备方报文模板一致 */
    private static final String TIME_PATTERN = "yyyy/MM/dd HH:mm:ss";

    /**
     * 触发召测并同步等待 RTU 应答入库（目前仅支持 37H 最新实时数据；38H 时段数据暂不开放）。
     * 展示层调用后阻塞等待：应答加报报文入库完成后返回 code=0，前端可直接刷新；
     * 超过超时时间仍未收到应答返回 code=1（召测无响应）。
     *
     * @param stcd  遥测站码（四站白名单内）
     * @param afn   指令参数：37=查询所有要素最新实时数据；38=查询指定要素时段数据（暂未开放）
     * @param stime 暂不生效（37H 下可不填，未填则留空）
     * @param etime 暂不生效（同上）
     * @return code=0 召测成功且应答数据已入库；code=1 失败（msg 说明原因）
     */
    public Map<String, Object> recall(String stcd, String afn, String stime, String etime) {
        // === 1. 白名单校验 ===
        String siteName = RECALL_SITE_MAP.get(stcd);
        if (siteName == null) {
            return error("站点不支持召测: " + stcd
                    + "（仅支持四站：北干渠进水闸/南干渠进水闸/毕岭节制闸/汪元节制闸）");
        }

        // === 2. afn 校验：目前仅开放 37H 最新实时数据 ===
        if (!"37".equals(afn)) {
            return error("afn 暂不支持: " + afn + "（目前仅支持 37=最新实时数据；38=时段数据暂未开放）");
        }

        // === 3. 构建 remotedata 指令报文 ===
        String ctime = new SimpleDateFormat(TIME_PATTERN).format(new Date());
        // 37H 查最新实时数据：stime/etime 先不填（留空），待设备方确认或上游报错后再调整
        String sendStime = (stime == null) ? "" : stime;
        String sendEtime = (etime == null) ? "" : etime;

        Map<String, String> cmd = new LinkedHashMap<>();
        cmd.put("gnm", "remotedata");
        cmd.put("afn", afn);
        cmd.put("stime", sendStime);
        cmd.put("etime", sendEtime);
        cmd.put("ctime", ctime);
        cmd.put("ycstcd", stcd);

        // === 4. 序列化为 JSON 报文 ===
        String message;
        try {
            message = objectMapper.writeValueAsString(cmd);
        } catch (JsonProcessingException e) {
            log.error("召测报文序列化失败: stcd={}, afn={}", stcd, afn, e);
            return error("召测报文序列化失败: " + e.getMessage());
        }

        // === 5. 同站并发召测互斥：该站已有等待中的召测则拒绝（与展示层按钮置灰形成双保险） ===
        CompletableFuture<Void> signal = new CompletableFuture<>();
        if (waiters.putIfAbsent(stcd, signal) != null) {
            log.warn("召测被拒(该站正在召测中): stcd={}({})", stcd, siteName);
            return error("该站正在召测中，请稍候再试（避免重复召测）");
        }
        long sentAt = System.currentTimeMillis();
        pendingRecall.put(stcd, sentAt);

        // === 6. 发送到召测队列（默认 exchange 直发队列名，队列由上游声明，本服务不声明） ===
        try {
            rabbitTemplate.convertAndSend("", recallQueue, message);
        } catch (Exception e) {
            waiters.remove(stcd);
            pendingRecall.remove(stcd);
            log.error("召测指令发送失败: stcd={}({}), queue={}", stcd, siteName, recallQueue, e);
            return error("召测指令发送失败: " + e.getMessage());
        }
        log.info("召测指令已发送: stcd={}({}), afn={}, queue={}, msg={}", stcd, siteName, afn, recallQueue, message);

        // === 7. 同步等待 RTU 应答加报入库（MonitorDataService 确认应答后通知） ===
        try {
            signal.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            waiters.remove(stcd);
            pendingRecall.remove(stcd);
            log.warn("召测无响应: stcd={}({}), 超时 {} 秒未收到 RTU 应答", stcd, siteName, timeoutSeconds);
            return error("召测无响应: " + timeoutSeconds + " 秒内未收到 RTU 应答（请检查上游是否消费召测队列/RTU 通信状态）");
        } catch (Exception e) {
            waiters.remove(stcd);
            pendingRecall.remove(stcd);
            log.error("召测等待应答异常: stcd={}", stcd, e);
            return error("召测等待应答异常: " + e.getMessage());
        }
        waiters.remove(stcd);

        // === 8. 应答已到达：留 2 秒入库缓冲，让同批次剩余 tag 报文（水位/流量/雨量等）入库完成 ===
        try {
            Thread.sleep(RESPONSE_SETTLE_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("召测入库缓冲等待被中断: stcd={}", stcd);
        }

        long now = System.currentTimeMillis();
        long elapsedSec = (now - sentAt) / 1000;
        // 记录召测成功时刻：展示层刷新页面后据此恢复"数据确认"按钮状态
        lastSuccess.put(stcd, now);
        log.info("召测完成: stcd={}({}), 应答已入库, 总耗时 {} 秒", stcd, siteName, elapsedSec);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", 0);
        result.put("msg", "召测成功，最新数据已入库，可刷新展示");
        Map<String, String> data = new LinkedHashMap<>();
        data.put("ycstcd", stcd);
        data.put("siteName", siteName);
        data.put("afn", afn);
        data.put("ctime", ctime);
        data.put("elapsedSeconds", String.valueOf(elapsedSec));
        data.put("queue", recallQueue);
        result.put("data", data);
        return result;
    }

    private Map<String, Object> error(String msg) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", 1);
        result.put("msg", msg);
        return result;
    }

    /**
     * 查询该站召测指令发出时间（只读，不消费记录）。供 MonitorDataService 判断报文是否召测应答。
     *
     * @param stcd 报文的遥测站码
     * @return 召测指令发出时间戳(ms)；无待确认召测返回 -1
     */
    public long getRecallSentTime(String stcd) {
        Long sent = pendingRecall.get(stcd);
        return sent == null ? -1 : sent;
    }

    /**
     * 确认召测应答已到达：清除待确认记录并唤醒同步等待中的接口线程。
     * 由 MonitorDataService 在识别到召测应答（ctime 判据通过）时调用。
     */
    public void notifyRecallResponse(String stcd) {
        pendingRecall.remove(stcd);
        CompletableFuture<Void> signal = waiters.remove(stcd);
        if (signal != null) {
            signal.complete(null);
        }
    }

    /**
     * 查询该站召测按钮状态（展示层刷新页面后调用，恢复按钮状态）。
     *
     * @return code=0 时 data.status = RECALLING（召测中）/ CONFIRMED（成功待用户确认）/ IDLE（空闲可召测）
     */
    public Map<String, Object> status(String stcd) {
        String siteName = RECALL_SITE_MAP.get(stcd);
        if (siteName == null) {
            return error("站点不支持召测: " + stcd
                    + "（仅支持四站：北干渠进水闸/南干渠进水闸/毕岭节制闸/汪元节制闸）");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", 0);
        result.put("msg", "ok");
        Map<String, String> data = new LinkedHashMap<>();
        data.put("stcd", stcd);
        data.put("siteName", siteName);
        data.put("status", currentStatus(stcd));
        result.put("data", data);
        return result;
    }

    /**
     * 用户点击"数据确认"按钮后调用：清除该站召测成功记录，按钮复位为召测样式。
     */
    public Map<String, Object> confirm(String stcd) {
        if (RECALL_SITE_MAP.get(stcd) == null) {
            return error("站点不支持召测: " + stcd
                    + "（仅支持四站：北干渠进水闸/南干渠进水闸/毕岭节制闸/汪元节制闸）");
        }
        lastSuccess.remove(stcd);
        log.info("召测已确认: stcd={}", stcd);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", 0);
        result.put("msg", "已确认，按钮可复位为召测样式");
        return result;
    }

    /**
     * 判定该站当前召测状态：RECALLING=正在召测（后端仍在等待应答）；
     * CONFIRMED=召测成功且在保留窗口内（展示层应显示绿色"数据确认"）；IDLE=空闲。
     */
    private String currentStatus(String stcd) {
        if (waiters.containsKey(stcd) || pendingRecall.containsKey(stcd)) {
            return "RECALLING";
        }
        Long successAt = lastSuccess.get(stcd);
        if (successAt != null && System.currentTimeMillis() - successAt < CONFIRMED_KEEP_WINDOW_MS) {
            return "CONFIRMED";
        }
        return "IDLE";
    }

    /**
     * 兜底清理：每分钟扫描待确认召测记录，防止异常路径（接口线程被杀等）导致的记录泄漏。
     * 正常情况下记录由 notifyRecallResponse 或接口超时路径清理。
     */
    @Scheduled(fixedDelay = 60000)
    public void scanRecallCleanup() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> e : pendingRecall.entrySet()) {
            long elapsed = now - e.getValue();
            if (elapsed > RECALL_CLEANUP_WINDOW_MS) {
                pendingRecall.remove(e.getKey());
                // 同步清理等待信号，防止异常路径残留导致该站状态永远 RECALLING（后续召测全被拒）。
                // 不 complete：若接口线程仍在等待（超时配置大于清理窗口的极端场景），
                // 让它按自身超时正常返回"无响应"，避免误报召测成功
                waiters.remove(e.getKey());
                log.warn("清理超时未确认的召测记录: stcd={}, 距发出已 {} 秒", e.getKey(), elapsed / 1000);
            }
        }
        // 清理超过保留窗口的召测成功记录（展示层未点"数据确认"则到期自动复位）
        for (Map.Entry<String, Long> e : lastSuccess.entrySet()) {
            if (now - e.getValue() > CONFIRMED_KEEP_WINDOW_MS) {
                lastSuccess.remove(e.getKey());
                log.info("清理过期的召测成功记录: stcd={}", e.getKey());
            }
        }
    }

    /**
     * 启动时检查召测队列是否存在、是否有消费者（默认 exchange 直发队列名，
     * 队列由上游声明：若队列不存在，指令消息会被静默丢弃不报错，接口只能等超时）。
     * 提前暴露问题，便于部署后第一时间发现上游未消费召测队列。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void checkRecallQueue() {
        try {
            java.util.Properties props = amqpAdmin.getQueueProperties(recallQueue);
            if (props != null) {
                log.info("召测队列 {} 存在: size={}, consumers={}", recallQueue,
                        props.get("QUEUE_MESSAGE_COUNT"), props.get("QUEUE_CONSUMER_COUNT"));
                if ("0".equals(String.valueOf(props.get("QUEUE_CONSUMER_COUNT")))) {
                    log.warn("!!!!! 召测队列 {} 当前无消费者，指令将无人处理，请检查上游报文服务 !!!!!", recallQueue);
                }
            } else {
                log.error("!!!!! 召测队列 {} 不存在，指令消息将被静默丢弃，召测必然超时 !!!!!", recallQueue);
            }
        } catch (Exception e) {
            log.error("检查召测队列状态失败: {}", e.getMessage());
        }
    }
}
