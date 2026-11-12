package com.qgyun.hltgq.hltgqmq.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 短信告警回调（对接 hltgq-site POST /sms/alarm-event）。
 *
 * 边界（2026-11-12 与 hltgq-site 对齐）：
 * - 仅水位/雨量阈值告警回调：metric=rain（typeCode #2#）/ water_level（typeCode #1#）；
 *   设备异常、站点失联、流量/开度/墒情/水质阈值告警不回调；
 * - 告警写入成功后立即回调一次（调用方在 insertAlert 返回 alertId 后触发），
 *   异步单线程执行，不阻塞 MQ 消费线程；连接/读取超时均 10 秒（可配，site 同步发短信实测 >3s）；
 * - 失败（超时/非 200/code!=0）只记一条 error 日志（带 alertId），不回滚告警、不重试，
 *   由 hltgq-site 侧每 5 分钟补偿扫描兜底补发；
 * - 同一 alertId 由 hltgq-site 保证只发一次短信，mq 侧不需要维护发送记录或去重。
 *
 * 请求体：
 * - 公共：alertId、metric、stcd（站点档案 iofhpi，按告警 site 反查）
 * - rain 额外：threshold（本次判定阈值 mm）、hour（本次判定实际窗口小时数）、value（窗口累积雨量 mm）
 *   注意 mq 雨量阈值口径为「当前水文日(08:00 切分)累积雨量」，故 hour = 报文时间距水文日起点的
 *   整小时数（向下取整、最少 1），不是固定 1/3/6 小时窗口；
 * - water_level 只传公共字段，幅度由 hltgq-site 按历史数据自行归纳。
 *
 * Token 由 hltgq-site 部署时提供（app.sms.alarm-event.token）；为空时启动日志提示并跳过回调。
 */
@Service
public class SmsAlarmNotifier {

    private static final Logger log = LoggerFactory.getLogger(SmsAlarmNotifier.class);

    private static final String SCHEMA = "\"qixiao-apaas\".";
    private static final String SITE_TABLE = SCHEMA + "t_auto_hltgq_5nw74_vnqqef";

    private static final String METRIC_RAIN = "rain";
    private static final String METRIC_WATER_LEVEL = "water_level";

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${app.sms.alarm-event.enabled:true}")
    private boolean enabled;

    @Value("${app.sms.alarm-event.url:http://10.68.18.4:18687/sms/alarm-event}")
    private String url;

    /** hltgq-site 提供的 X-Sms-Token；为空则跳过回调（部署时填写） */
    @Value("${app.sms.alarm-event.token:}")
    private String token;

    /** 连接/读取超时（毫秒）：site 同步发短信耗时约 3s+（2026-11-12 实测首条回调 Read timed out），
     *  故读超时需大于 site 发送耗时；连接与读取共用此值（内网连接瞬时，10s 只影响 site 卡死场景） */
    @Value("${app.sms.alarm-event.timeout-ms:10000}")
    private int timeoutMs;

    /**
     * 单线程 + 有界队列：告警有抑制窗与一日去重，量很小；串行回调避免线程膨胀，
     * 队列满直接丢弃并记 error（hltgq-site 有补偿扫描兜底），绝不阻塞调用方线程。
     */
    private final ExecutorService pool = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(200),
            r -> {
                Thread t = new Thread(r, "sms-alarm-notify");
                t.setDaemon(true);
                return t;
            },
            // 队列满直接丢弃，error 日志带 alertId 便于事后定位（hltgq-site 补偿扫描兜底补发）
            (r, e) -> log.error("短信告警回调队列已满, 本次丢弃(由 hltgq-site 补偿扫描兜底补发), alertId={}",
                    r instanceof NotifyTask ? ((NotifyTask) r).alertId : "-"));

    @PostConstruct
    public void init() {
        boolean hasToken = token != null && !token.trim().isEmpty();
        log.info("短信告警回调配置: enabled={}, url={}, timeout={}ms, token={}",
                enabled, url, timeoutMs, hasToken ? "已配置" : "未配置(回调将跳过)");
        if (enabled && !hasToken) {
            log.warn("短信告警回调 token 未配置(app.sms.alarm-event.token), 本次启动不会发送短信告警");
        }
    }

    @PreDestroy
    public void shutdown() {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(3, TimeUnit.SECONDS)) {
                pool.shutdownNow();
                log.warn("停机时仍有未完成的短信告警回调, 已强制关闭(由 hltgq-site 补偿扫描兜底)");
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** 回调任务（带 alertId，供拒绝策略日志定位） */
    private final class NotifyTask implements Runnable {
        final String alertId;
        final String metric;
        final String siteId;
        final Double threshold;
        final double value;
        final Timestamp tm;

        NotifyTask(String alertId, String metric, String siteId, Double threshold, double value, Timestamp tm) {
            this.alertId = alertId;
            this.metric = metric;
            this.siteId = siteId;
            this.threshold = threshold;
            this.value = value;
            this.tm = tm;
        }

        @Override
        public void run() {
            try {
                doNotify(alertId, metric, siteId, threshold, value, tm);
            } catch (Exception e) {
                log.error("短信告警回调异常, alertId={}: {}", alertId, e.getMessage());
            }
        }
    }

    /**
     * 阈值告警落库成功后调用（异步、失败仅记日志）。
     * 仅处理水位/雨量两类；其余类型直接返回。
     *
     * @param alertId   告警表主键 id（hltgq-site 幂等键）
     * @param typeCode  阈值类型字典：#1#水位 / #2#雨量（其余忽略）
     * @param siteId    站点 id（回调时反查 iofhpi 得 stcd）
     * @param threshold 本次判定阈值（雨量必传，mm）
     * @param value     本次判定实测值（雨量=窗口累积雨量，mm）
     * @param tm        告警报文测量时间（用于计算雨量窗口小时数）
     */
    public void notifyThresholdAlert(String alertId, String typeCode, String siteId,
                                     Double threshold, double value, Timestamp tm) {
        if (!enabled) {
            return;
        }
        String metric;
        if (AlertService.TYPE_RAINFALL.equals(typeCode)) {
            metric = METRIC_RAIN;
        } else if (AlertService.TYPE_WATER_LEVEL.equals(typeCode)) {
            metric = METRIC_WATER_LEVEL;
        } else {
            return; // 流量/开度/墒情/水质阈值告警不发短信
        }
        if (token == null || token.trim().isEmpty()) {
            log.debug("短信告警回调跳过(token未配置): alertId={}, metric={}, site={}", alertId, metric, siteId);
            return;
        }
        if (alertId == null || siteId == null) {
            return;
        }
        try {
            pool.execute(new NotifyTask(alertId, metric, siteId, threshold, value, tm));
        } catch (Exception e) {
            // 理论上只有线程池已关闭才会抛（如停机中），丢弃即可
            log.error("短信告警回调提交失败, alertId={}: {}", alertId, e.getMessage());
        }
    }

    private void doNotify(String alertId, String metric, String siteId,
                          Double threshold, double value, Timestamp tm) {
        String stcd = lookupStcd(siteId);
        if (stcd == null) {
            log.error("短信告警回调跳过, 站点无有效编码: alertId={}, site={}", alertId, siteId);
            return;
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("alertId", alertId);
        body.put("metric", metric);
        body.put("stcd", stcd);
        if (METRIC_RAIN.equals(metric)) {
            if (threshold == null || threshold <= 0) {
                log.error("短信告警回调跳过, 雨量阈值缺失: alertId={}, site={}", alertId, siteId);
                return;
            }
            body.put("threshold", compactNumber(threshold));
            body.put("hour", rainWindowHours(tm));
            body.put("value", compactNumber(value));
        }
        String payload;
        try {
            payload = objectMapper.writeValueAsString(body);
        } catch (Exception e) {
            log.error("短信告警回调序列化失败, alertId={}: {}", alertId, e.getMessage());
            return;
        }
        try {
            String resp = post(payload);
            JsonNode node = objectMapper.readTree(resp);
            int code = node.path("code").asInt(-1);
            if (code != 0) {
                log.error("短信告警回调失败, alertId={}, metric={}, stcd={}, code={}, resp={}",
                        alertId, metric, stcd, code, abbreviate(resp));
                return;
            }
            // sent/reason 仅供联调观察，不做分支（发送结果由 hltgq-site 保证）
            log.info("短信告警回调成功, alertId={}, metric={}, stcd={}, sent={}, reason={}, body={}",
                    alertId, metric, stcd, node.path("data").path("sent").asBoolean(false),
                    node.path("data").path("reason").asText(""), payload);
        } catch (Exception e) {
            log.error("短信告警回调失败, alertId={}, metric={}, stcd={}: {}",
                    alertId, metric, stcd, e.getMessage());
        }
    }

    /** 站点 id → iofhpi(stcd)；查不到返回 null。告警频率低，直接查库（不缓存，避免站点换站后 stcd 过期） */
    private String lookupStcd(String siteId) {
        try {
            List<String> rows = jdbcTemplate.queryForList(
                    "SELECT iofhpi FROM " + SITE_TABLE + " WHERE id = ?", String.class, siteId);
            if (rows.isEmpty() || rows.get(0) == null || rows.get(0).trim().isEmpty()) {
                return null;
            }
            return rows.get(0).trim();
        } catch (Exception e) {
            log.warn("短信告警回调查询站点编码失败, site={}: {}", siteId, e.getMessage());
            return null;
        }
    }

    private String post(String payload) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        try {
            conn.setRequestMethod("POST");
            conn.setConnectTimeout(timeoutMs);
            conn.setReadTimeout(timeoutMs);
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            conn.setRequestProperty("X-Sms-Token", token.trim());
            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.getBytes(StandardCharsets.UTF_8));
            }
            int httpCode = conn.getResponseCode();
            InputStream in = httpCode >= 200 && httpCode < 300 ? conn.getInputStream() : conn.getErrorStream();
            String resp = readAll(in);
            if (httpCode != 200) {
                throw new IllegalStateException("HTTP " + httpCode + ", resp=" + abbreviate(resp));
            }
            return resp;
        } finally {
            conn.disconnect();
        }
    }

    private static String readAll(InputStream in) throws Exception {
        if (in == null) {
            return "";
        }
        try (InputStream is = in; ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buf = new byte[2048];
            int n;
            while ((n = is.read(buf)) != -1) {
                bos.write(buf, 0, n);
            }
            return new String(bos.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    /**
     * 雨量判定窗口小时数（分钟口径，向下取整、最少 1）：
     * 起点与 MonitorDataService.queryHydroDayBaseDyp 同基准——用服务器当前时间 now 定「当前水文日
     * 08:00 起点」（value 的基线就是按 now 算的），终点用报文测量时间 tm；
     * 正常实时上报时两者一致；报文 tm 落在上一水文日（延迟补传/跨 08:00）时，value 已因
     * 基线错位退化为 0/无效，此时按 now 兜底（与基线同窗口）并留痕。
     * 避免旧实现（以 tm 定水文日）在跨日时 hour 与 value 窗口完全脱节。
     */
    static int rainWindowHours(Timestamp tm) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime start = now.toLocalDate().atTime(8, 0);
        if (now.isBefore(start)) {
            start = start.minusDays(1);
        }
        LocalDateTime t = tm == null ? now : tm.toLocalDateTime();
        if (t.isBefore(start)) {
            log.debug("雨量告警报文时间早于当前水文日起点, 窗口按处理时刻兜底: tm={}, start={}", tm, start);
            t = now;
        }
        long minutes = Duration.between(start, t).toMinutes();
        return (int) Math.max(minutes / 60, 1);
    }

    /** 数字精简：整数值输出整数（30 而非 30.0），其余四舍五入 2 位（45.2、0.35） */
    private static Object compactNumber(double v) {
        double r = Math.round(v * 100d) / 100d;
        if (r == Math.rint(r) && Math.abs(r) < 1e15) {
            return (long) r;
        }
        return r;
    }

    private static String abbreviate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() <= 300 ? s : s.substring(0, 300) + "...";
    }
}
