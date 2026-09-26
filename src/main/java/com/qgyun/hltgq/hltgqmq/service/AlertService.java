package com.qgyun.hltgq.hltgqmq.service;

import com.qgyun.hltgq.hltgqmq.util.IdGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 告警信息入库服务
 *
 * 三类告警统一入库 t_auto_hltgq_water_alert，阈值配置读 t_auto_hltgq_water_threshold：
 * 1. 设备通讯异常：哨兵值 FFFFFFFF → 内容"{站点名}-{指标} 设备异常！"，级别 #3#
 * 2. 站点失联：24h 无任何入库数据 → 内容"{站点名} 连续24小时未收到任何报文，疑似站点失联！"，级别 #3#
 * 3. 阈值越界：按「站点+类型(+指标zb)」实时读配置，单级警戒值、方向按 alarmdir（含等号）触发，级别 #3#
 *
 * 告警类型(type)字典（告警表字段，区分业务类别，与阈值指标类型 TYPE_* 无关）：
 * #1# 阈值超限（阈值越界类） #2# 异常告警（设备异常/站点失联类）
 *
 * 告警语义（审计定版；2026-09-26 按《阈值告警比对-给hltgq-mq》细化，并按展示层评审修订）：
 * - 新增默认 status=#1#(未确认)；阈值类告警按抑制窗防重复（alarm.suppress-minutes，默认 30 分钟），
 *   抑制键为「站点+类型(+指标zb)」语义（以 content 前缀=站点名+指标名 实现，与阈值数值/方向无关）；
 * - 数据恢复正常时自动置 status=#4#(已关闭)并重新计数（再超限立即告警），平台侧也可手动维护状态；
 *   自动关闭仅限系统产生且未人工介入的行（created_by='SYSTEM' 且 status ∈ #1#/#2#，防覆盖人工态）；
 * - 阈值判定：警戒值 threshold 由设置页保证必填>0；guarantee/num/device 本期预留不启用；
 * - 阈值表读取：类型统一 COALESCE(NULLIF(zvieyb,''),type,'') 子串匹配（CONCAT 模糊，见 findThreshold）；
 *   字典 #1#水位 #2#雨量 #3#流量 #4#开度 #7#墒情(10/20/30cm三层) #8#水质(2026-09-26 纳入，按 zb 七项)；
 *   多指标类型（#7#/#8#）按「站点 × 类型 × zb」定位（zb=监测数据表列名），单指标类型 zb 为空。
 *   注意：告警表 type 与阈值表 zvieyb 是两套字典，展示层/平台查询时勿混淆。
 */
@Service
public class AlertService {

    private static final Logger log = LoggerFactory.getLogger(AlertService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private WorkOrderService workOrderService;

    /** 人大金仓 schema（带双引号，因为含连字符） */
    private static final String SCHEMA = "\"qixiao-apaas\".";

    private static final String ALERT_TABLE     = SCHEMA + "t_auto_hltgq_water_alert";
    private static final String THRESHOLD_TABLE = SCHEMA + "t_auto_hltgq_water_threshold";

    @Value("${app.corp-code:hltgq}")
    private String corpCode;

    /** 重复告警抑制时长(分钟)：同站点同条件持续超限时两次告警的最小间隔；环境值≤0 时按 1 分钟兜底防刷屏 */
    @Value("${alarm.suppress-minutes:30}")
    private int suppressMinutes;

    /** 告警级别字典：#3#严重（阈值告警统一 #3#） */
    private static final String LEVEL_SEVERE   = "#3#";
    /** 处理状态：#1#未确认(新增默认) #2#已确认 #4#已关闭(恢复)；自动关闭白名单仅 #1#/#2# */
    private static final String STATUS_UNCONFIRMED = "#1#";
    private static final String STATUS_CONFIRMED   = "#2#";
    private static final String STATUS_CLOSED      = "#4#";

    /** 告警类型字典：#1#阈值超限(阈值越界类) #2#异常告警(设备异常/站点失联类)；与阈值指标类型 TYPE_* 是两套字典 */
    public static final String ALERT_TYPE_THRESHOLD = "#1#";
    public static final String ALERT_TYPE_ABNORMAL  = "#2#";

    /** 阈值类型字典（zvieyb 子串匹配；多指标类型再按 zb 定位到具体指标行） */
    public static final String TYPE_WATER_LEVEL   = "#1#";
    public static final String TYPE_RAINFALL      = "#2#";
    public static final String TYPE_FLOW          = "#3#";
    public static final String TYPE_GATE          = "#4#";
    public static final String TYPE_SOIL          = "#7#";
    public static final String TYPE_WATER_QUALITY = "#8#";

    /** 告警表有效列名缓存（动态列适配，与各入库表一致） */
    private Set<String> alertColumns = Collections.emptySet();

    /** 告警编号序号：按秒重置、同秒递增（code=GJ+yyyyMMddHHmmss+3位序号） */
    private final AtomicInteger codeSeq = new AtomicInteger();
    private volatile long codeLastSecond = 0;

    private static final DecimalFormat VALUE_FORMAT = new DecimalFormat("#.##");

    /** 数值格式化（线程安全包装：DecimalFormat 非线程安全，多消费线程并发会错乱） */
    private static String fmt(double v) {
        synchronized (VALUE_FORMAT) {
            return VALUE_FORMAT.format(v);
        }
    }

    /** 线程安全的告警编号时间格式（SimpleDateFormat 非线程安全，多消费线程并发会错乱） */
    private static final java.time.format.DateTimeFormatter CODE_TIME_FORMATTER =
            java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    /** 阈值查询失败哨兵（区别于 null=确无配置）：评估时跳过本次判定，不新增也不关闭告警 */
    private static final Map<String, Object> QUERY_FAILED = Collections.emptyMap();

    /** 批量阈值查询失败哨兵（区别于 null=确无配置） */
    private static final Map<String, Map<String, Object>> ROWS_QUERY_FAILED = Collections.emptyMap();

    /** 抑制查询失败时的进程内兜底限流：key=site|device|content前缀(站点名+指标名) → 最近一次阈值告警时间(ms)，
     *  防存储抖动期间抑制机制失效导致告警风暴（展示层评审 #9，替代原 fail-open） */
    private final ConcurrentMap<String, Long> recentThresholdAlertMemMs = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        try {
            String sql = "SELECT column_name FROM information_schema.columns " +
                    "WHERE table_schema = 'qixiao-apaas' AND table_name = 't_auto_hltgq_water_alert'";
            List<String> cols = jdbcTemplate.queryForList(sql, String.class);
            alertColumns = new HashSet<>();
            for (String col : cols) {
                alertColumns.add(col.toLowerCase());
            }
            log.info("已加载告警表列名元数据: {} 列", alertColumns.size());
        } catch (Exception e) {
            log.error("加载告警表列名元数据失败", e);
        }
    }

    // ======================== 设备异常告警 ========================

    /**
     * 设备通讯异常（哨兵值 FFFFFFFF）：新增告警，内容"{站点名}-{指标} 设备异常！"，级别 #3#。
     * 同一未关闭告警（site+device+content）不重复新增。
     */
    public void reportDeviceError(String siteId, String deviceId, String siteName,
                                  String metric, Timestamp tm) {
        if (siteId == null || deviceId == null) return;
        String content = siteName + "-" + metric + " 设备异常！";
        if (existsUnclosed(siteId, deviceId, content)) {
            return;
        }
        insertAlert(siteId, deviceId, content, LEVEL_SEVERE, ALERT_TYPE_ABNORMAL, tm,
                WorkOrderService.WORK_TYPE_REPAIR);
        log.warn("新增设备异常告警: site={}, device={}, content={}", siteId, deviceId, content);
    }

    /** 设备数据恢复正常：关闭该站点-设备-指标的设备异常告警（content 精确匹配） */
    public void closeDeviceError(String siteId, String deviceId, String siteName, String metric) {
        if (siteId == null || deviceId == null) return;
        String content = siteName + "-" + metric + " 设备异常！";
        int rows = closeByContent(siteId, deviceId, content);
        if (rows > 0) {
            log.info("设备恢复正常, 关闭告警 {} 条: site={}, device={}, content={}", rows, siteId, deviceId, content);
        }
        // 告警恢复 → 同步自动关闭对应工单
        workOrderService.closeByContent(siteId, deviceId, content);
    }

    // ======================== 站点失联告警 ========================

    /** 站点失联（24h 无入库数据）：新增告警，级别 #3#；该站已有未关闭失联告警则不重复新增 */
    public void reportOffline(String siteId, String deviceId, String siteName, Timestamp tm) {
        if (siteId == null || deviceId == null || siteName == null) return;
        if (existsUnclosedLike(siteId, "%失联%")) {
            return;
        }
        String content = siteName + " 连续24小时未收到任何报文，疑似站点失联！";
        insertAlert(siteId, deviceId, content, LEVEL_SEVERE, ALERT_TYPE_ABNORMAL, tm,
                WorkOrderService.WORK_TYPE_REPAIR);
        log.warn("新增站点失联告警: site={}, content={}", siteId, content);
    }

    /** 站点恢复通信（收到报文标在线时）：关闭该站全部失联告警 */
    public void closeOfflineAlerts(String siteId) {
        if (siteId == null) return;
        try {
            // 白名单（展示层评审 #3）：仅关闭系统产生且未人工介入的行（created_by='SYSTEM' 且 status ∈ #1#/#2#）
            String sql = "UPDATE " + ALERT_TABLE +
                    " SET status = ?, updated_at = ?, updated_by = 'SYSTEM' " +
                    " WHERE site = ? AND content LIKE '%失联%' AND created_by = 'SYSTEM' AND status IN (?, ?)";
            int rows = jdbcTemplate.update(sql, STATUS_CLOSED,
                    new Timestamp(System.currentTimeMillis()), siteId, STATUS_UNCONFIRMED, STATUS_CONFIRMED);
            if (rows > 0) {
                log.info("站点恢复通信, 关闭失联告警 {} 条: site={}", rows, siteId);
            }
        } catch (Exception e) {
            log.debug("关闭失联告警失败, site={}: {}", siteId, e.getMessage());
        }
        // 告警恢复 → 同步自动关闭该站失联类工单
        workOrderService.closeByLike(siteId, "%失联%");
    }

    // ======================== 阈值告警 ========================

    /**
     * 单指标类型阈值判定入口（水位/雨量/流量/开度/闸站水位等，zb 恒为空）。
     */
    public void evaluateThreshold(String siteId, String deviceId, String typeCode,
                                  String siteName, String metric, double value, Timestamp tm) {
        evaluateThreshold(siteId, deviceId, typeCode, siteName, metric, null, value, tm);
    }

    /**
     * 阈值判定（2026-09-26 细化设计，仅对正常数值调用，-999/-9991 等异常值由调用方排除）：
     * 1. 匹配键「站点 + 类型(+指标 zb)」实时读阈值表（无缓存），页面编辑保存后下一条数据即生效；
     * 2. 单级警戒值 threshold，方向以行上 alarmdir 为准（#1#高于 / #2#低于，均含等号触发），
     *    为空回退默认方向（墒情各层与水质溶解氧 dox 为低于，其余高于）；
     * 3. 无配置（或警戒值缺失/非正，防御）→ 静默跳过，同时关闭遗留阈值告警防配置删除后悬挂；
     * 4. 触发 → 新增告警（抑制窗内不重复，见 reportThresholdAlert）；
     *    正常区间 → 关闭该设备该指标全部阈值类告警（恢复由 updated_by=SYSTEM 标记，抑制计数重置）；
     * 5. 查询失败 → 跳过本次评估（不新增也不关闭，避免误关遗留告警），下次上报重试。
     *    多指标类型（#7#/#8#）由调用方走 evaluateThresholdBatch 一次查库、内存按 zb 匹配。
     * 告警内容不含当前值（当前值每报变化会导致去重失效、异常期间反复新增），
     * 文案只体现阈值本身："{站点}{指标}高于警戒值X{单位}！/低于警戒值X{单位}！"，同阈值下同文案。
     */
    public void evaluateThreshold(String siteId, String deviceId, String typeCode,
                                  String siteName, String metric, String zb,
                                  double value, Timestamp tm) {
        if (siteId == null || deviceId == null) return;
        Map<String, Object> th = findThreshold(siteId, typeCode, zb);
        if (th == QUERY_FAILED) {
            return; // 查询失败不等同于无配置：跳过本次评估，防止误关遗留告警
        }
        evaluateThresholdRow(siteId, deviceId, typeCode, siteName, zb, metric, value, tm, th);
    }

    /**
     * 批量阈值评估（2026-09-26 展示层评审 #10）：一次读取该站该类型全部阈值行，
     * 内存按 zb 匹配，避免水质七项/墒情三层把一条报文放大成 3~7 次 SELECT；
     * 查询失败 → 跳过全部指标（与单项口径一致），实时性不变（仍为每报实时读）。
     */
    public void evaluateThresholdBatch(String siteId, String deviceId, String typeCode,
                                       String siteName, Timestamp tm, List<ThresholdMetric> metrics) {
        if (siteId == null || deviceId == null || metrics == null || metrics.isEmpty()) return;
        Map<String, Map<String, Object>> rowsByZb = findThresholdRows(siteId, typeCode);
        if (rowsByZb == ROWS_QUERY_FAILED) {
            return; // 查询失败：跳过全部指标，防止误关遗留告警
        }
        for (ThresholdMetric m : metrics) {
            Map<String, Object> th = rowsByZb == null ? null : rowsByZb.get(m.zb == null ? "" : m.zb);
            evaluateThresholdRow(siteId, deviceId, typeCode, siteName, m.zb, m.metric, m.value, tm, th);
        }
    }

    /** 批量评估的单个指标项：zb=阈值表指标列（=监测数据表列名），metric=告警文案指标名，value=读数 */
    public static final class ThresholdMetric {
        final String zb;
        final String metric;
        final double value;
        public ThresholdMetric(String zb, String metric, double value) {
            this.zb = zb;
            this.metric = metric;
            this.value = value;
        }
    }

    /** 单指标判定与动作（单条/批量共用核心）：th 为匹配到的阈值行（null=无配置），执行触发/恢复关闭/防悬挂 */
    private void evaluateThresholdRow(String siteId, String deviceId, String typeCode,
                                      String siteName, String zb, String metric, double value,
                                      Timestamp tm, Map<String, Object> th) {
        Double threshold = th == null ? null : toDbDouble(th.get("threshold"));
        if (!isPositive(threshold)) {
            // 无配置或警戒值缺失/非正（设置页保证必填>0，此处防御）：静默跳过 + 防悬挂
            closeThresholdAlerts(siteId, deviceId, siteName, metric);
            return;
        }
        String alarmdir = th.get("alarmdir") != null ? String.valueOf(th.get("alarmdir")).trim() : "";
        // 方向：#2#低于（含等号）；#1#高于（含等号）；空按类型/指标默认方向兜底
        boolean below = "#2#".equals(alarmdir) || (alarmdir.isEmpty() && isBelowByDefault(typeCode, zb));
        boolean triggered = below ? value <= threshold : value >= threshold;
        if (triggered) {
            reportThresholdAlert(siteId, deviceId, siteName, metric,
                    (below ? "低于警戒值" : "高于警戒值") + fmt(threshold),
                    unitFor(typeCode, zb), tm);
        } else {
            // 正常区间：关闭该设备该指标全部阈值类告警
            closeThresholdAlerts(siteId, deviceId, siteName, metric);
        }
    }

    private void reportThresholdAlert(String siteId, String deviceId, String siteName,
                                      String metric, String desc, String unit, Timestamp tm) {
        String prefix = siteName + metric;
        String content = prefix + desc + unit + "！";
        if (isSuppressed(siteId, deviceId, prefix)) {
            log.debug("阈值告警抑制窗内不重复: site={}, device={}, prefix={}", siteId, deviceId, prefix);
            return;
        }
        // 无论落库成败均记录进程内时间：存储抖动导致库内不可检时由内存兜底限流（fail-closed 防风暴）
        recentThresholdAlertMemMs.put(siteId + "|" + deviceId + "|" + prefix, System.currentTimeMillis());
        insertAlert(siteId, deviceId, content, LEVEL_SEVERE, ALERT_TYPE_THRESHOLD, tm,
                WorkOrderService.WORK_TYPE_EMERGENCY);
        log.warn("新增阈值告警: site={}, device={}, content={}", siteId, deviceId, content);
    }

    /**
     * 阈值告警重复抑制（2026-09-26 细化设计 + 展示层评审修订）：
     * 抑制键为「站点 + 类型(+指标 zb)」语义——以 content 前缀（站点名+指标名）实现，与阈值数值/方向无关，
     * 页面改阈值不影响持续超限的抑制连续性（文案本身含指标名、不含实时读数，各指标天然隔离）。
     * 取同站点-设备-前缀的最近一条告警：
     * - 已由系统自动恢复关闭(updated_by=SYSTEM) → 计数已重置，再超限立即告警（放行）；
     * - 其余（仍告警中 / 人为处置关闭）→ 创建时间在抑制窗（alarm.suppress-minutes 分钟，≤0 按 1 兜底）
     *   内则抑制；持续超限超过抑制窗后再次告警（提醒），人为处置后的告警窗内不重复打扰。
     * 查询失败 → 退化为进程内最小间隔限流（展示层评审 #9：原 fail-open 在存储抖动时会告警风暴，
     * 改为按内存记录的最近告警时间在窗内抑制、超窗放行）。
     */
    private boolean isSuppressed(String siteId, String deviceId, String prefix) {
        String memKey = siteId + "|" + deviceId + "|" + prefix;
        long windowMs = Math.max(suppressMinutes, 1) * 60000L;
        try {
            String sql = "SELECT status, updated_by, created_at FROM " + ALERT_TABLE +
                    " WHERE site = ? AND device = ? AND content LIKE ? ORDER BY created_at DESC LIMIT 1";
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, siteId, deviceId, prefix + "%");
            if (rows.isEmpty()) {
                recentThresholdAlertMemMs.remove(memKey);
                return false;
            }
            Map<String, Object> last = rows.get(0);
            String status = last.get("status") != null ? String.valueOf(last.get("status")) : "";
            String updatedBy = last.get("updated_by") != null ? String.valueOf(last.get("updated_by")) : "";
            if (STATUS_CLOSED.equals(status) && "SYSTEM".equals(updatedBy)) {
                recentThresholdAlertMemMs.remove(memKey);
                return false; // 数值恢复正常自动关闭 → 重新计数（再超限立即告警）
            }
            Object created = last.get("created_at");
            long createdMs = created instanceof java.util.Date ? ((java.util.Date) created).getTime() : -1L;
            boolean suppressed = createdMs > 0 && System.currentTimeMillis() - createdMs < windowMs;
            if (suppressed) {
                recentThresholdAlertMemMs.put(memKey, createdMs); // 与库内状态同步，供存储抖动兜底
            }
            return suppressed;
        } catch (Exception e) {
            Long lastMs = recentThresholdAlertMemMs.get(memKey);
            boolean suppressed = lastMs != null && System.currentTimeMillis() - lastMs < windowMs;
            log.warn("阈值抑制查询失败, 按进程内限流{}: site={}, device={}, prefix={}, err={}",
                    suppressed ? "抑制" : "放行", siteId, deviceId, prefix, e.getMessage());
            return suppressed;
        }
    }

    /** 阈值文案单位（按告警对象）：#1#水位 m、#2#雨量 mm、#3#流量 m³/s、#4#闸门 m、#7#墒情 %、#8#水质 mg/L（水温 ℃） */
    private static String unitFor(String typeCode, String zb) {
        if (TYPE_WATER_LEVEL.equals(typeCode)) return "m";
        if (TYPE_RAINFALL.equals(typeCode)) return "mm";
        if (TYPE_FLOW.equals(typeCode)) return "m³/s";
        if (TYPE_GATE.equals(typeCode)) return "m";
        if (TYPE_SOIL.equals(typeCode)) return "%";
        if (TYPE_WATER_QUALITY.equals(typeCode)) return "wt".equals(zb) ? "℃" : "mg/L";
        return "";
    }

    /**
     * 默认触发方向（行上 alarmdir 为空时的兜底，2026-09-26 细化设计）：
     * 墒情各层（10/20/30cm）与水质溶解氧 dox 为「低于」，其余类型/指标「高于」。
     */
    private static boolean isBelowByDefault(String typeCode, String zb) {
        if (TYPE_SOIL.equals(typeCode)) return true;
        return TYPE_WATER_QUALITY.equals(typeCode) && "dox".equals(zb);
    }

    /**
     * 关闭该设备该指标的阈值类告警。按 content 前缀(站点名+指标)限定范围：
     * 同一 RTU 设备常上报多指标（水位+雨量+流量…），仅关闭本指标告警，
     * 否则任一指标正常会把该设备其他指标的阈值告警误关（告警闪断）；
     * 关键词过滤只命阈值类文案（警戒值；保证值/设计值为历史分级文案兼容），不波及设备异常告警。
     * 触发场景：数值回落正常区间 / 该指标无阈值配置（防配置删除后告警悬挂）。
     * 白名单（展示层评审 #3）：仅关闭系统产生且未人工介入的行（created_by='SYSTEM' 且 status ∈ #1#/#2#），
     * 人为态（已确认处理/已关闭）与人工创建的行一律不动；
     * 关闭即 updated_by=SYSTEM → 抑制计数重置；同文案多行（抑制窗提醒产生的重复行）一并关闭。
     */
    private void closeThresholdAlerts(String siteId, String deviceId, String siteName, String metric) {
        try {
            String sql = "UPDATE " + ALERT_TABLE +
                    " SET status = ?, updated_at = ?, updated_by = 'SYSTEM' " +
                    " WHERE site = ? AND device = ? AND content LIKE ? " +
                    " AND created_by = 'SYSTEM' AND status IN (?, ?) " +
                    " AND (content LIKE '%保证值%' OR content LIKE '%警戒值%' OR content LIKE '%设计值%')";
            int rows = jdbcTemplate.update(sql, STATUS_CLOSED,
                    new Timestamp(System.currentTimeMillis()),
                    siteId, deviceId, siteName + metric + "%", STATUS_UNCONFIRMED, STATUS_CONFIRMED);
            if (rows > 0) {
                log.info("阈值告警恢复正常, 关闭 {} 条: site={}, device={}, metric={}", rows, siteId, deviceId, metric);
            }
        } catch (Exception e) {
            log.debug("关闭阈值告警失败, site={}, device={}, metric={}: {}", siteId, deviceId, metric, e.getMessage());
        }
        // 告警恢复 → 同步关闭同条件的阈值类工单
        workOrderService.closeThreshold(siteId, deviceId, siteName + metric + "%",
                " AND (content LIKE '%保证值%' OR content LIKE '%警戒值%' OR content LIKE '%设计值%')");
    }

    /**
     * 查站点-类型(-指标)匹配的阈值配置（实时读取无缓存，页面编辑保存后下一条数据即生效）。
     * 阈值按「站点 × 类型(+zb)」唯一，与设备无关（同站同指标的全部设备/测点共用一条线）；
     * 类型读取统一 COALESCE(NULLIF("zvieyb",''),"type",'') 子串匹配：新配置行 type 恒空、
     * 历史多值行(如 #1#|#2#)同时命中各类型；旧列 type 仅兜底。
     * 模糊匹配必须 LIKE CONCAT('%', ?, '%')——KingbaseES 实测 '%'||?||'%' 绑定参数会静默失效退化为全表。
     * 单指标类型 zb 传 null（不加指标条件）；多指标类型（#7#/#8#）按 zb=监测数据表列名定位到具体指标行；
     * LIMIT 1（设置侧保证同「站点+类型+指标」最多一行）。
     * 查询失败返回 QUERY_FAILED（跳过本次评估，下次上报重试），区别于 null=确无配置。
     */
    private Map<String, Object> findThreshold(String siteId, String typeCode, String zb) {
        try {
            String sql = "SELECT threshold, alarmdir FROM " + THRESHOLD_TABLE +
                    " WHERE \"site\" = ? AND COALESCE(NULLIF(\"zvieyb\", ''), \"type\", '') LIKE CONCAT('%', ?, '%')" +
                    (zb != null ? " AND \"zb\" = ?" : "") +
                    " LIMIT 1";
            List<Map<String, Object>> rows = zb != null
                    ? jdbcTemplate.queryForList(sql, siteId, typeCode, zb)
                    : jdbcTemplate.queryForList(sql, siteId, typeCode);
            return rows.isEmpty() ? null : rows.get(0);
        } catch (Exception e) {
            log.warn("查询阈值配置失败, site={}, type={}, zb={}: {}", siteId, typeCode, zb, e.getMessage());
            return QUERY_FAILED; // 下次上报重试
        }
    }

    /**
     * 批量读取该站该类型的全部阈值行（展示层评审 #10），返回 zb → 阈值行 的映射
     * （单指标类型 zb 为空串键；同键多行时首行生效，设置侧保证同「站点+类型+指标」唯一）。
     * 无配置返回 null；查询失败返回 QUERY_FAILED（跳过本次全部评估）。
     */
    private Map<String, Map<String, Object>> findThresholdRows(String siteId, String typeCode) {
        try {
            String sql = "SELECT threshold, alarmdir, \"zb\" FROM " + THRESHOLD_TABLE +
                    " WHERE \"site\" = ? AND COALESCE(NULLIF(\"zvieyb\", ''), \"type\", '') LIKE CONCAT('%', ?, '%')";
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, siteId, typeCode);
            if (rows.isEmpty()) {
                return null;
            }
            Map<String, Map<String, Object>> byZb = new HashMap<>();
            for (Map<String, Object> row : rows) {
                Object zbVal = row.get("zb");
                String key = zbVal == null ? "" : String.valueOf(zbVal).trim();
                if (!byZb.containsKey(key)) {
                    byZb.put(key, row);
                }
            }
            return byZb;
        } catch (Exception e) {
            log.warn("批量查询阈值配置失败, site={}, type={}: {}", siteId, typeCode, e.getMessage());
            return ROWS_QUERY_FAILED;
        }
    }

    // ======================== 基础方法 ========================

    /** 新增告警行（动态列适配，status 默认 #1# 未确认，type 区分阈值超限/异常告警，time=报文测量时间；
     *  workOrderType 落随生工单的 qjulvf 类型字典值） */
    private void insertAlert(String siteId, String deviceId, String content, String level,
                             String alertType, Timestamp tm, String workOrderType) {
        try {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            Map<String, Object> fm = new LinkedHashMap<>();
            String alertId = IdGenerator.generate();
            fm.put("id",         alertId);
            fm.put("corp_code",  corpCode);
            fm.put("created_at", now);
            fm.put("created_by", "SYSTEM");
            fm.put("updated_at", now);
            fm.put("updated_by", "SYSTEM");
            fm.put("code",       genAlertCode(now));
            fm.put("site",       siteId);
            fm.put("device",     deviceId);
            fm.put("content",    content);
            fm.put("type",       alertType);
            fm.put("level",      level);
            fm.put("status",     STATUS_UNCONFIRMED);
            fm.put("time",       tm != null ? tm : now);

            StringBuilder cols = new StringBuilder();
            StringBuilder phs = new StringBuilder();
            List<Object> vals = new ArrayList<>();
            for (Map.Entry<String, Object> e : fm.entrySet()) {
                if (!alertColumns.isEmpty() && !alertColumns.contains(e.getKey().toLowerCase())) {
                    continue; // 列不存在则跳过（动态列适配）
                }
                if (cols.length() > 0) { cols.append(", "); phs.append(", "); }
                // level/type 为 SQL 保留字或方言关键字，需加双引号（其余列名与库内小写列名一致，无需引号）
                String col = ("level".equalsIgnoreCase(e.getKey()) || "type".equalsIgnoreCase(e.getKey()))
                        ? "\"" + e.getKey() + "\"" : e.getKey();
                cols.append(col);
                phs.append("?");
                vals.add(e.getValue());
            }
            String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", ALERT_TABLE, cols, phs);
            jdbcTemplate.update(sql, vals.toArray());
            // 告警新增成功 → 自动生成工单（alert 字段存告警ID形成精确关联，qjulvf 落工单类型字典）
            workOrderService.createIfAbsent(alertId, siteId, deviceId, deriveWorkOrderTitle(content),
                    content, workOrderType);
        } catch (Exception e) {
            log.error("告警入库失败, site={}, device={}, content={}: {}", siteId, deviceId, content, e.getMessage());
        }
    }

    /** 工单标题派生：告警内容去掉结尾"！"（与告警 content 同源，工单关闭时按 content 匹配） */
    private static String deriveWorkOrderTitle(String content) {
        return (content != null && content.endsWith("！"))
                ? content.substring(0, content.length() - 1) : content;
    }

    /** 同站点-设备-内容且未关闭的告警是否存在（新增去重） */
    private boolean existsUnclosed(String siteId, String deviceId, String content) {
        try {
            String sql = "SELECT COUNT(*) FROM " + ALERT_TABLE +
                    " WHERE site = ? AND device = ? AND content = ? AND status IS DISTINCT FROM ?";
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, siteId, deviceId, content, STATUS_CLOSED);
            return count != null && count > 0;
        } catch (Exception e) {
            log.debug("告警去重检查失败, 放行: {}", e.getMessage());
            return false;
        }
    }

    /** 站点级未关闭模糊匹配（失联告警去重用） */
    private boolean existsUnclosedLike(String siteId, String pattern) {
        try {
            String sql = "SELECT COUNT(*) FROM " + ALERT_TABLE +
                    " WHERE site = ? AND content LIKE ? AND status IS DISTINCT FROM ?";
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, siteId, pattern, STATUS_CLOSED);
            return count != null && count > 0;
        } catch (Exception e) {
            log.debug("告警去重检查失败, 放行: {}", e.getMessage());
            return false;
        }
    }

    /** 精确 content 关闭（设备异常告警恢复用；白名单同 closeThresholdAlerts：仅系统产生且 status ∈ #1#/#2#） */
    private int closeByContent(String siteId, String deviceId, String content) {
        try {
            String sql = "UPDATE " + ALERT_TABLE +
                    " SET status = ?, updated_at = ?, updated_by = 'SYSTEM' " +
                    " WHERE site = ? AND device = ? AND content = ? AND created_by = 'SYSTEM' AND status IN (?, ?)";
            return jdbcTemplate.update(sql, STATUS_CLOSED,
                    new Timestamp(System.currentTimeMillis()), siteId, deviceId, content,
                    STATUS_UNCONFIRMED, STATUS_CONFIRMED);
        } catch (Exception e) {
            log.debug("关闭告警失败: {}", e.getMessage());
            return 0;
        }
    }

    /**
     * 告警编号：GJ + yyyyMMddHHmmss + 3位序号（按秒重置、同秒递增）。
     * 应用重启后同秒序号可能撞车，概率极低且 code 无唯一约束时无害。
     */
    private String genAlertCode(Timestamp tm) {
        long sec = tm.getTime() / 1000;
        int seq;
        synchronized (codeSeq) {
            if (sec != codeLastSecond) {
                codeLastSecond = sec;
                codeSeq.set(0);
            }
            seq = codeSeq.incrementAndGet();
        }
        return "GJ" + tm.toLocalDateTime().format(CODE_TIME_FORMATTER) + String.format("%03d", seq);
    }

    /** 阈值警戒值有效判定：填了且 >0 才启用（0/空=未设置，不判定，放权给客户） */
    private static boolean isPositive(Double v) {
        return v != null && v > 0;
    }

    /** Object → Double（兼容 Number 与文本），转换失败返回 null */
    private static Double toDbDouble(Object value) {
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).doubleValue();
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
