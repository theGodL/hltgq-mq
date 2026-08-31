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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 告警信息入库服务
 *
 * 三类告警统一入库 t_auto_hltgq_water_alert，阈值配置读 t_auto_hltgq_water_threshold：
 * 1. 设备通讯异常：哨兵值 FFFFFFFF → 内容"{站点名}-{指标} 设备异常！"，级别 #3#
 * 2. 站点失联：24h 无任何入库数据 → 内容"{站点名} 连续24小时未收到任何报文，疑似站点失联！"，级别 #3#
 * 3. 阈值越界：低于保证值/高于警戒值 #3#，高于设计值 #4#（无阈值配置不判定）
 *
 * 告警语义（审计定版）：
 * - 新增默认 status=#1#(未确认)，同一未关闭告警(content相同)不重复新增；
 * - 数据恢复正常时自动置 status=#4#(已关闭)，平台侧也可手动维护状态（自动+手动结合）；
 * - 阈值判定"放权给客户"：threshold/guarantee/num 填了才判定（>0 视为启用，空/0 不判定）；
 * - type 字典：#1#水位 #2#雨量 #3#流量 #4#闸门 #7#墒情（#5#视频由大华项目维护、#8#水质无数据源，本项目不处理）。
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

    /** 告警级别字典：#3#严重 #4#特别严重 */
    private static final String LEVEL_SEVERE   = "#3#";
    private static final String LEVEL_CRITICAL = "#4#";
    /** 处理状态：#1#未确认(新增默认) #4#已关闭(恢复) */
    private static final String STATUS_UNCONFIRMED = "#1#";
    private static final String STATUS_CLOSED      = "#4#";

    /** 阈值类型字典（type 单选，一指标一条记录） */
    public static final String TYPE_WATER_LEVEL = "#1#";
    public static final String TYPE_RAINFALL    = "#2#";
    public static final String TYPE_FLOW        = "#3#";
    public static final String TYPE_GATE        = "#4#";
    public static final String TYPE_SOIL        = "#7#";

    /** 告警表有效列名缓存（动态列适配，与各入库表一致） */
    private Set<String> alertColumns = Collections.emptySet();

    /** 阈值配置缓存（5分钟过期，客户平台改阈值后最多5分钟生效） */
    private final ConcurrentMap<String, ThresholdEntry> thresholdCache = new ConcurrentHashMap<>();
    private static final long THRESHOLD_CACHE_TTL_MS = 5 * 60 * 1000L;

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

    /** 阈值缓存条目（row=null 表示无配置，也缓存避免反复查库） */
    private static final class ThresholdEntry {
        final Map<String, Object> row;
        final long expireAt;
        ThresholdEntry(Map<String, Object> row, long expireAt) {
            this.row = row;
            this.expireAt = expireAt;
        }
    }

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
        insertAlert(siteId, deviceId, content, LEVEL_SEVERE, tm);
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
        insertAlert(siteId, deviceId, content, LEVEL_SEVERE, tm);
        log.warn("新增站点失联告警: site={}, content={}", siteId, content);
    }

    /** 站点恢复通信（收到报文标在线时）：关闭该站全部失联告警 */
    public void closeOfflineAlerts(String siteId) {
        if (siteId == null) return;
        try {
            String sql = "UPDATE " + ALERT_TABLE +
                    " SET status = ?, updated_at = ?, updated_by = 'SYSTEM' " +
                    " WHERE site = ? AND content LIKE '%失联%' AND status IS DISTINCT FROM ?";
            int rows = jdbcTemplate.update(sql, STATUS_CLOSED,
                    new Timestamp(System.currentTimeMillis()), siteId, STATUS_CLOSED);
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
     * 阈值判定（仅对正常数值调用，-9991/-999 等异常值由调用方排除）：
     * 低于保证值 → #3#；高于设计值 → #4#；高于警戒值 → #3#；
     * 数值回到正常区间 → 关闭该设备该指标的阈值类告警；
     * 无阈值配置 → 不新增（关闭该设备该指标遗留的阈值类告警，防止配置被删除后告警悬挂）。
     *
     * 告警内容不含当前值（当前值每报变化会导致去重失效、异常期间反复新增），
     * 文案只体现阈值本身："{站点}{指标}低于保证值X！"等，同阈值下异常期间仅一条告警。
     */
    public void evaluateThreshold(String siteId, String deviceId, String typeCode,
                                  String siteName, String metric, double value, Timestamp tm) {
        if (siteId == null || deviceId == null) return;
        Map<String, Object> th = findThreshold(siteId, deviceId, typeCode);
        if (th == null) {
            closeThresholdAlerts(siteId, deviceId, siteName, metric);
            return;
        }
        Double guarantee = toDbDouble(th.get("guarantee"));
        Double threshold = toDbDouble(th.get("threshold"));
        Double num       = toDbDouble(th.get("num"));

        if (isPositive(guarantee) && value < guarantee) {
            reportThresholdAlert(siteId, deviceId, siteName, metric,
                    "低于保证值" + fmt(guarantee), unitFor(typeCode), LEVEL_SEVERE, tm);
            return;
        }
        if (isPositive(num) && value > num) {
            reportThresholdAlert(siteId, deviceId, siteName, metric,
                    "高于设计值" + fmt(num), unitFor(typeCode), LEVEL_CRITICAL, tm);
            return;
        }
        if (isPositive(threshold) && value > threshold) {
            reportThresholdAlert(siteId, deviceId, siteName, metric,
                    "高于警戒值" + fmt(threshold), unitFor(typeCode), LEVEL_SEVERE, tm);
            return;
        }
        // 正常区间：关闭该设备该指标全部阈值类告警
        closeThresholdAlerts(siteId, deviceId, siteName, metric);
    }

    private void reportThresholdAlert(String siteId, String deviceId, String siteName,
                                      String metric, String desc, String unit, String level, Timestamp tm) {
        String content = siteName + metric + desc + unit + "！";
        if (existsUnclosed(siteId, deviceId, content)) {
            return;
        }
        insertAlert(siteId, deviceId, content, level, tm);
        log.warn("新增阈值告警: site={}, device={}, content={}", siteId, deviceId, content);
    }

    /** 阈值文案单位（按告警对象）：#1#水位 m、#2#雨量 mm、#3#流量 m³/s、#4#闸门 m、#7#墒情 % */
    private static String unitFor(String typeCode) {
        if (TYPE_WATER_LEVEL.equals(typeCode)) return "m";
        if (TYPE_RAINFALL.equals(typeCode)) return "mm";
        if (TYPE_FLOW.equals(typeCode)) return "m³/s";
        if (TYPE_GATE.equals(typeCode)) return "m";
        if (TYPE_SOIL.equals(typeCode)) return "%";
        return "";
    }

    /**
     * 关闭该设备该指标的阈值类告警。按 content 前缀(站点名+指标)限定范围：
     * 同一 RTU 设备常上报多指标（水位+雨量+流量…），仅关闭本指标告警，
     * 否则任一指标正常会把该设备其他指标的阈值告警误关（告警闪断）。
     */
    private void closeThresholdAlerts(String siteId, String deviceId, String siteName, String metric) {
        try {
            String sql = "UPDATE " + ALERT_TABLE +
                    " SET status = ?, updated_at = ?, updated_by = 'SYSTEM' " +
                    " WHERE site = ? AND device = ? AND content LIKE ? AND status IS DISTINCT FROM ? " +
                    " AND (content LIKE '%保证值%' OR content LIKE '%警戒值%' OR content LIKE '%设计值%')";
            int rows = jdbcTemplate.update(sql, STATUS_CLOSED,
                    new Timestamp(System.currentTimeMillis()), siteId, deviceId,
                    siteName + metric + "%", STATUS_CLOSED);
            if (rows > 0) {
                log.info("指标恢复正常, 关闭阈值告警 {} 条: site={}, device={}, metric={}", rows, siteId, deviceId, metric);
            }
        } catch (Exception e) {
            log.debug("关闭阈值告警失败, site={}, device={}, metric={}: {}", siteId, deviceId, metric, e.getMessage());
        }
        // 告警恢复 → 同步自动关闭该站该设备该指标的阈值类工单
        workOrderService.closeThreshold(siteId, deviceId, siteName + metric + "%",
                " AND (content LIKE '%保证值%' OR content LIKE '%警戒值%' OR content LIKE '%设计值%')");
    }

    /**
     * 查站点-设备-类型匹配的阈值配置（type 单选、一指标一条记录）：
     * device 精确匹配优先，站级（device 为空）记录其次；5 分钟缓存。
     */
    private Map<String, Object> findThreshold(String siteId, String deviceId, String typeCode) {
        String key = siteId + "|" + deviceId + "|" + typeCode;
        ThresholdEntry cached = thresholdCache.get(key);
        if (cached != null && cached.expireAt > System.currentTimeMillis()) {
            return cached.row;
        }
        Map<String, Object> row = null;
        try {
            String sql = "SELECT threshold, guarantee, num FROM " + THRESHOLD_TABLE +
                    " WHERE site = ? AND type = ? AND (device = ? OR device IS NULL OR device = '') " +
                    " ORDER BY (device = ?) DESC LIMIT 1";
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, siteId, typeCode, deviceId, deviceId);
            if (!rows.isEmpty()) {
                row = rows.get(0);
            }
        } catch (Exception e) {
            log.debug("查询阈值配置失败, site={}, device={}, type={}: {}", siteId, deviceId, typeCode, e.getMessage());
        }
        thresholdCache.put(key, new ThresholdEntry(row, System.currentTimeMillis() + THRESHOLD_CACHE_TTL_MS));
        return row;
    }

    // ======================== 基础方法 ========================

    /** 新增告警行（动态列适配，status 默认 #1# 未确认，time=报文测量时间） */
    private void insertAlert(String siteId, String deviceId, String content, String level, Timestamp tm) {
        try {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            Map<String, Object> fm = new LinkedHashMap<>();
            fm.put("id",         IdGenerator.generate());
            fm.put("corp_code",  corpCode);
            fm.put("created_at", now);
            fm.put("created_by", "SYSTEM");
            fm.put("updated_at", now);
            fm.put("updated_by", "SYSTEM");
            fm.put("code",       genAlertCode(now));
            fm.put("site",       siteId);
            fm.put("device",     deviceId);
            fm.put("content",    content);
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
                // level 为 SQL 保留字，需加双引号（其余列名与库内小写列名一致，无需引号）
                String col = "level".equalsIgnoreCase(e.getKey()) ? "\"level\"" : e.getKey();
                cols.append(col);
                phs.append("?");
                vals.add(e.getValue());
            }
            String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", ALERT_TABLE, cols, phs);
            jdbcTemplate.update(sql, vals.toArray());
            // 告警新增成功 → 自动生成工单（同一未关闭告警不重复生成，工单侧去重双保险）
            workOrderService.createIfAbsent(siteId, deviceId, deriveWorkOrderTitle(content), content);
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

    /** 精确 content 关闭（设备异常告警恢复用） */
    private int closeByContent(String siteId, String deviceId, String content) {
        try {
            String sql = "UPDATE " + ALERT_TABLE +
                    " SET status = ?, updated_at = ?, updated_by = 'SYSTEM' " +
                    " WHERE site = ? AND device = ? AND content = ? AND status IS DISTINCT FROM ?";
            return jdbcTemplate.update(sql, STATUS_CLOSED,
                    new Timestamp(System.currentTimeMillis()), siteId, deviceId, content, STATUS_CLOSED);
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

    /** 阈值字段有效判定：填了且 >0 才启用（0/空=未设置，不判定，放权给客户） */
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
