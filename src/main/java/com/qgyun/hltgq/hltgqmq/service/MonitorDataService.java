package com.qgyun.hltgq.hltgqmq.service;

import com.qgyun.hltgq.hltgqmq.util.IdGenerator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.scheduling.annotation.Scheduled;

/**
 * MonitorData 消息处理服务
 * 解析MQ报文并入库到对应的人大金仓数据表
 */
@Service
public class MonitorDataService {

    private static final Logger log = LoggerFactory.getLogger(MonitorDataService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 人大金仓 schema（带双引号，因为含连字符）
     */
    private static final String SCHEMA = "\"qixiao-apaas\".";

    /**
     * 系统常量
     */
    @Value("${app.corp-code:hltgq}")
    private String corpCode;

    /**
     * tag -> 数据库全限定表名 映射
     */
    private static final Map<String, String> TAG_TABLE_MAP = new LinkedHashMap<>();
    static {
        TAG_TABLE_MAP.put("msgInfo",    SCHEMA + "t_auto_hltgq_water_msg_info");
        TAG_TABLE_MAP.put("volInfo",    SCHEMA + "t_auto_hltgq_water_vol_info");
        TAG_TABLE_MAP.put("wtInfo",     SCHEMA + "t_auto_hltgq_water_wt_nfo");
        TAG_TABLE_MAP.put("riverInfo",  SCHEMA + "t_auto_hltgq_water_river_info");
        TAG_TABLE_MAP.put("nmIspInfo",  SCHEMA + "t_auto_hltgq_water_nmisp_info");
        TAG_TABLE_MAP.put("pcpInfo",    SCHEMA + "t_auto_hltgq_water_pcp_info");
        TAG_TABLE_MAP.put("rainInfo",   SCHEMA + "t_auto_hltgq_water_rain_info");
        TAG_TABLE_MAP.put("gatesInfo",  SCHEMA + "t_auto_hltgq_water_gate");
        TAG_TABLE_MAP.put("gateInfo",   SCHEMA + "t_auto_hltgq_water_gate");
    }

    /** gate 表全限定名（riverInfo 闸站水位路由目标） */
    private static final String GATE_TABLE = SCHEMA + "t_auto_hltgq_water_gate";

    /** 设备表全限定名 */
    private static final String DEVICE_TABLE = SCHEMA + "t_auto_hltgq_water_device";

    /** tag → 设备类型后缀（用于构造设备名称: 站点名+设备类型+#） */
    private static final Map<String, String> TAG_DEVICE_TYPE_MAP = new LinkedHashMap<>();
    static {
        TAG_DEVICE_TYPE_MAP.put("msgInfo",   "通信终端");
        TAG_DEVICE_TYPE_MAP.put("volInfo",   "遥测终端");
        TAG_DEVICE_TYPE_MAP.put("wtInfo",    "流量计");
        TAG_DEVICE_TYPE_MAP.put("riverInfo", "水位计");
        TAG_DEVICE_TYPE_MAP.put("nmIspInfo", "水质监测仪");
        TAG_DEVICE_TYPE_MAP.put("pcpInfo",   "理化监测仪");
        TAG_DEVICE_TYPE_MAP.put("rainInfo",  "雨量计");
        TAG_DEVICE_TYPE_MAP.put("gatesInfo", "闸位计");
        TAG_DEVICE_TYPE_MAP.put("gateInfo",  "闸位计");
    }

    /**
     * 表列名缓存：表名(不含schema) -> 该表所有列名的集合
     */
    private final Map<String, Set<String>> tableColumnsCache = new HashMap<>();

    /** 设备名 → device_id 缓存（首次查/建后缓存） */
    private final ConcurrentMap<String, String> deviceCache = new ConcurrentHashMap<>();

    /** site_id → 站点名称 缓存 */
    private final ConcurrentMap<String, String> siteNameCache = new ConcurrentHashMap<>();

    /** stcd → site_id 缓存 */
    private final ConcurrentMap<String, String> stcdSiteCache = new ConcurrentHashMap<>();

    /**
     * 启动时从 information_schema 加载所有目标表的列名
     */
    @PostConstruct
    public void initTableColumns() {
        try {
            String sql = "SELECT table_name, column_name FROM information_schema.columns " +
                    "WHERE table_schema = 'qixiao-apaas' " +
                    "AND table_name LIKE 't_auto_hltgq_water_%'";
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
            for (Map<String, Object> row : rows) {
                String tableName = ((String) row.get("table_name")).toLowerCase();
                String columnName = ((String) row.get("column_name")).toLowerCase();
                tableColumnsCache.computeIfAbsent(tableName, k -> new HashSet<>()).add(columnName);
            }
            log.info("已加载 {} 张表的列名元数据: {}", tableColumnsCache.size(), tableColumnsCache.keySet());
        } catch (Exception e) {
            log.error("加载表列名元数据失败，INSERT将跳过列名校验（可能引发入库错误）", e);
        }
    }

    /**
     * 处理MQ消息：解析JSON -> 查找site -> 动态拼装INSERT -> 入库
     */
    public void process(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            String tag = root.get("tag").asText();
            JsonNode entity = root.get("entity");

            if (entity == null) {
                log.warn("消息缺少entity字段: {}", message);
                return;
            }

            String tableName = TAG_TABLE_MAP.get(tag);
            if (tableName == null) {
                log.warn("未知的tag类型: {}, 消息: {}", tag, message);
                return;
            }

            String stcd = entity.has("STCD") ? entity.get("STCD").asText() : "";
            if (stcd.isEmpty()) {
                log.warn("消息缺少STCD字段: {}", message);
                return;
            }

            // 通过stcd查找站点ID，找不到则丢弃该报文
            String site = lookupSite(stcd);
            if (site == null) {
                log.warn("未找到stcd对应的site, 丢弃报文: stcd={}, tag={}", stcd, tag);
                return;
            }

            // 收到报文即标记站点在线
            markSiteOnline(site);

            // 提取纯表名用于列名校验（去掉 schema 前缀）
            String pureTableName = tableName.substring(tableName.indexOf('.') + 1);
            Set<String> validColumns = tableColumnsCache.getOrDefault(pureTableName, Collections.emptySet());

            // 动态构建INSERT字段映射
            Map<String, Object> fieldMap = new LinkedHashMap<>();

            // === NOT NULL 系统字段（所有表统一要求） ===
            fieldMap.put("id",          IdGenerator.generate());
            fieldMap.put("corp_code",   corpCode);
            Timestamp now = new Timestamp(System.currentTimeMillis());
            fieldMap.put("created_at",  now);
            fieldMap.put("created_by",  "SYSTEM");
            fieldMap.put("updated_at",  now);
            fieldMap.put("updated_by",  "SYSTEM");

            // === 业务字段（仅当列存在时才添加） ===
            if (isValidColumn(validColumns, "site"))   fieldMap.put("site", site);
            if (isValidColumn(validColumns, "device")) fieldMap.put("device", lookupOrCreateDeviceByStcd(stcd, site, tag));
            if (isValidColumn(validColumns, "stcd"))   fieldMap.put("stcd", stcd);

            // 遍历entity字段，转为小写作为数据库列名（仅当列存在时才添加）
            Iterator<String> fieldNames = entity.fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                String lowerKey = fieldName.toLowerCase();
                // stcd 已添加，避免重复
                if ("stcd".equals(lowerKey)) {
                    continue;
                }
                if (!isValidColumn(validColumns, lowerKey)) {
                    continue;
                }
                JsonNode valueNode = entity.get(fieldName);
                fieldMap.put(lowerKey, convertValue(valueNode));
            }

            // === riverInfo 路由 ===
            if ("riverInfo".equals(tag)) {
                if (hasValue(entity, "Z")) {
                    // Z有值 → 通用水位站，继续走下方 river_info 表入库
                } else if ((hasValue(entity, "Z1") && entity.get("Z1").asDouble() > 0)
                        || (hasValue(entity, "Z2") && entity.get("Z2").asDouble() > 0)) {
                    // Z1/Z2有正值 → 闸站水位，写入 gate 表，跳过 river_info
                    insertGateFromRiverInfo(entity, site, stcd);
                    return;
                } else {
                    // Z/Z1/Z2全空 → 无有效水位数据
                    log.debug("riverInfo 无水位数据, 跳过入库, stcd={}", stcd);
                    return;
                }
            }
            // === gateInfo / gatesInfo 路由：闸门开度，直接写入 gate 表 ===
            if ("gatesInfo".equals(tag) || "gateInfo".equals(tag)) {
                insertGateFromGatesInfo(entity, site, stcd);
                return;
            }
            // === msgInfo 条件入库：msg为空则跳过 ===
            if ("msgInfo".equals(tag) && !hasValue(entity, "MSG")) {
                log.debug("msgInfo 报文(MSG)为空, 跳过入库, stcd={}", stcd);
                return;
            }
            // === volInfo 条件入库：VOL为空或≤0视为无效 ===
            if ("volInfo".equals(tag)) {
                if (!hasValue(entity, "VOL") || entity.get("VOL").asDouble() <= 0) {
                    log.debug("volInfo 无有效电压数据, 跳过入库, stcd={}", stcd);
                    return;
                }
            }
            // === wtInfo 条件入库：Q为空或≤0视为无效流量 ===
            if ("wtInfo".equals(tag)) {
                if (!hasValue(entity, "Q") || entity.get("Q").asDouble() <= 0) {
                    log.debug("wtInfo 无有效流量数据, 跳过入库, stcd={}", stcd);
                    return;
                }
            }
            // === rainInfo 条件入库：DRP为空或≤0视为无有效雨量 ===
            if ("rainInfo".equals(tag)) {
                if (!hasValue(entity, "DRP") || entity.get("DRP").asDouble() <= 0) {
                    log.debug("rainInfo 无有效雨量数据, 跳过入库, stcd={}", stcd);
                    return;
                }
            }
            // === nmIspInfo / pcpInfo 暂不录入，等待后续设备接入 ===
            if ("nmIspInfo".equals(tag) || "pcpInfo".equals(tag)) {
                log.debug("{} 暂不录入, 等待设备接入, stcd={}", tag, stcd);
                return;
            }

            // === 计算字段：水位涨幅、累计降雨 ===
            if ("riverInfo".equals(tag)) {
                computeWaterLevelRise1h(fieldMap, stcd, validColumns);
            } else if ("rainInfo".equals(tag)) {
                computeRainfall(fieldMap, entity, stcd, validColumns);
            }

            // 构建 INSERT SQL
            StringBuilder columns = new StringBuilder();
            StringBuilder placeholders = new StringBuilder();
            List<Object> values = new ArrayList<>();

            for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
                if (columns.length() > 0) {
                    columns.append(", ");
                    placeholders.append(", ");
                }
                columns.append(entry.getKey());
                placeholders.append("?");
                values.add(entry.getValue());
            }

            String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                    tableName, columns.toString(), placeholders.toString());

            jdbcTemplate.update(sql, values.toArray());
            log.info("数据入库成功: tag={}, stcd={}, table={}", tag, stcd, tableName);

        } catch (Exception e) {
            log.error("数据入库失败: {}", message, e);
            // 不抛出异常，避免阻塞MQ消费
        }
    }

    /**
     * 检查列名是否在表的有效列集合中（缓存为空时兜底放行）
     */
    private boolean isValidColumn(Set<String> validColumns, String columnName) {
        // 缓存未加载时放行所有列（兜底）
        if (validColumns.isEmpty()) {
            return true;
        }
        return validColumns.contains(columnName);
    }

    /**
     * 通过stcd查找站点ID（带缓存）
     * 查询 t_auto_hltgq_5nw74_vnqqef 表，iofhpi = stcd，返回 id
     */
    private String lookupSite(String stcd) {
        return stcdSiteCache.computeIfAbsent(stcd, s -> {
            try {
                String sql = "SELECT id FROM " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef WHERE iofhpi = ?";
                List<String> results = jdbcTemplate.queryForList(sql, String.class, s);
                if (results != null && !results.isEmpty()) {
                    return results.get(0);
                }
            } catch (Exception e) {
                log.warn("查找site失败, stcd={}, 错误: {}", s, e.getMessage());
            }
            return null;
        });
    }

    /**
     * 按 tag + site 查找或创建设备，返回 t_auto_hltgq_water_device.id (带缓存)
     * 设备名称 = 站点名 + 设备类型（如"南山寺节制闸雨量计"），首次查不到则自动创建
     */
    private String lookupOrCreateDeviceByStcd(String stcd, String siteId, String tag) {
        String deviceType = TAG_DEVICE_TYPE_MAP.getOrDefault(tag, tag);
        // 设备名称格式：站点名+设备类型+#，如"集岭管理所雨量计#"，闸孔类已在 insertGateFromGatesInfo 单独命名
        String deviceName = getSiteName(siteId) + deviceType + "#";
        return lookupOrCreateDeviceByName(deviceName, siteId);
    }

    /** 按设备名称查找或创建（共享缓存） */
    private String lookupOrCreateDeviceByName(String deviceName, String siteId) {
        return deviceCache.computeIfAbsent(deviceName, name -> {
            try {
                String sql = "SELECT id FROM " + DEVICE_TABLE + " WHERE name = ?";
                List<String> results = jdbcTemplate.queryForList(sql, String.class, name);
                if (results != null && !results.isEmpty()) {
                    return results.get(0);
                }
            } catch (Exception e) {
                log.warn("查找设备失败, name={}, 错误: {}", name, e.getMessage());
            }

            // 不存在则创建
            try {
                String deviceId = IdGenerator.generate();
                Timestamp now = new Timestamp(System.currentTimeMillis());
                String sql = "INSERT INTO " + DEVICE_TABLE +
                        " (id, corp_code, created_at, created_by, updated_at, updated_by, name, site) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                jdbcTemplate.update(sql, deviceId, corpCode, now, "SYSTEM", now, "SYSTEM", name, siteId);
                log.info("已自动创建遥测设备: name={}, deviceId={}", name, deviceId);
                return deviceId;
            } catch (Exception e) {
                log.error("创建遥测设备失败, name={}, 错误: {}", name, e.getMessage());
                return null;
            }
        });
    }

    /** 根据 site_id 查站点名称（带缓存） */
    private String getSiteName(String siteId) {
        return siteNameCache.computeIfAbsent(siteId, id -> {
            try {
                String sql = "SELECT zzkaec FROM " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef WHERE id = ?";
                List<String> results = jdbcTemplate.queryForList(sql, String.class, id);
                if (results != null && !results.isEmpty() && results.get(0) != null) {
                    return results.get(0);
                }
            } catch (Exception e) {
                log.warn("查找站点名称失败, siteId={}, 错误: {}", id, e.getMessage());
            }
            return id; // 兜底，用 site_id 作为名称
        });
    }

    /**
     * riverInfo 闸站路由：将 Z1→up_z、Z2→down_z 写入 gate 表（站级水位，gate_no=0）
     */
    private void insertGateFromRiverInfo(JsonNode entity, String siteId, String stcd) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Set<String> gateCols = tableColumnsCache.getOrDefault("t_auto_hltgq_water_gate", Collections.emptySet());

        Map<String, Object> fm = new LinkedHashMap<>();
        fm.put("id",         IdGenerator.generate());
        fm.put("corp_code",  corpCode);
        fm.put("created_at", now);
        fm.put("created_by", "SYSTEM");
        fm.put("updated_at", now);
        fm.put("updated_by", "SYSTEM");
        if (gateCols.isEmpty() || gateCols.contains("site"))     fm.put("site",     siteId);
        if (gateCols.isEmpty() || gateCols.contains("device"))   fm.put("device",   lookupOrCreateDeviceByStcd(stcd, siteId, "riverInfo"));
        if (gateCols.isEmpty() || gateCols.contains("stcd"))     fm.put("stcd",     stcd);
        if (gateCols.isEmpty() || gateCols.contains("gate_no"))  fm.put("gate_no",  "0");
        if (gateCols.isEmpty() || gateCols.contains("tm"))       fm.put("tm",       now);
        if (gateCols.isEmpty() || gateCols.contains("date"))     fm.put("date",     now);
        if (gateCols.isEmpty() || gateCols.contains("ctime"))    fm.put("ctime",    now);
        if (gateCols.isEmpty() || gateCols.contains("status"))   fm.put("status",   "#1#");
        if (hasValue(entity, "Z1") && entity.get("Z1").asDouble() > 0
                && (gateCols.isEmpty() || gateCols.contains("up_z")))
            fm.put("up_z", entity.get("Z1").asDouble());
        if (hasValue(entity, "Z2") && entity.get("Z2").asDouble() > 0
                && (gateCols.isEmpty() || gateCols.contains("down_z")))
            fm.put("down_z", entity.get("Z2").asDouble());

        StringBuilder cols = new StringBuilder();
        StringBuilder phs = new StringBuilder();
        List<Object> vals = new ArrayList<>();
        for (Map.Entry<String, Object> e : fm.entrySet()) {
            if (cols.length() > 0) { cols.append(", "); phs.append(", "); }
            cols.append(e.getKey());
            phs.append("?");
            vals.add(e.getValue());
        }

        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                GATE_TABLE, cols.toString(), phs.toString());
        jdbcTemplate.update(sql, vals.toArray());

        log.info("riverInfo闸站数据入库成功: stcd={}, Z1={}, Z2={}, table={}",
                stcd,
                hasValue(entity, "Z1") ? entity.get("Z1").asText() : "null",
                hasValue(entity, "Z2") ? entity.get("Z2").asText() : "null",
                GATE_TABLE);
    }

    /**
     * gatesInfo 闸门开度路由：将 Gates1/2/3 → gate 表多行（闸孔级开度，gate_no=1/2/3）
     * 每闸孔独立设备，命名规则与 MQTT 一致：{站点名}{闸孔号}#
     */
    private void insertGateFromGatesInfo(JsonNode entity, String siteId, String stcd) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Set<String> gateCols = tableColumnsCache.getOrDefault("t_auto_hltgq_water_gate", Collections.emptySet());
        String siteName = getSiteName(siteId);

        int inserted = 0;
        for (int i = 1; i <= 10; i++) {
            String fieldName = "Gates" + i;
            if (!hasValue(entity, fieldName)) break; // 连续编号，遇到第一个null就停止

            double openDegree = entity.get(fieldName).asDouble();
            // -999 表示无信号，跳过该闸孔（不创建设备）
            if (openDegree == -999) continue;

            String deviceName = siteName + i + "#";
            String deviceId = lookupOrCreateDeviceByName(deviceName, siteId);

            Map<String, Object> fm = new LinkedHashMap<>();
            fm.put("id",          IdGenerator.generate());
            fm.put("corp_code",   corpCode);
            fm.put("created_at",  now);
            fm.put("created_by",  "SYSTEM");
            fm.put("updated_at",  now);
            fm.put("updated_by",  "SYSTEM");
            if (gateCols.isEmpty() || gateCols.contains("site"))        fm.put("site",         siteId);
            if (gateCols.isEmpty() || gateCols.contains("device"))      fm.put("device",       deviceId);
            if (gateCols.isEmpty() || gateCols.contains("stcd"))        fm.put("stcd",         stcd);
            if (gateCols.isEmpty() || gateCols.contains("gate_no"))     fm.put("gate_no",      String.valueOf(i));
            if (gateCols.isEmpty() || gateCols.contains("tm"))          fm.put("tm",           now);
            if (gateCols.isEmpty() || gateCols.contains("date"))        fm.put("date",         now);
            if (gateCols.isEmpty() || gateCols.contains("ctime"))       fm.put("ctime",        now);
            if (gateCols.isEmpty() || gateCols.contains("status"))      fm.put("status",       "#1#");
            if (gateCols.isEmpty() || gateCols.contains("open_degree")) fm.put("open_degree",  openDegree);

            StringBuilder cols = new StringBuilder();
            StringBuilder phs = new StringBuilder();
            List<Object> vals = new ArrayList<>();
            for (Map.Entry<String, Object> e : fm.entrySet()) {
                if (cols.length() > 0) { cols.append(", "); phs.append(", "); }
                cols.append(e.getKey());
                phs.append("?");
                vals.add(e.getValue());
            }

            String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                    GATE_TABLE, cols.toString(), phs.toString());
            jdbcTemplate.update(sql, vals.toArray());
            inserted++;
        }

        log.info("gatesInfo闸门开度入库成功: stcd={}, 闸孔数={}, table={}", stcd, inserted, GATE_TABLE);
    }

    /**
     * 计算1h水位涨幅（cm）：当前Z - 1小时前Z，写入 water_level_rise1h 字段
     */
    private void computeWaterLevelRise1h(Map<String, Object> fieldMap, String stcd, Set<String> validColumns) {
        if (!isValidColumn(validColumns, "water_level_rise1h")) return;

        Object zObj = fieldMap.get("z");
        if (!(zObj instanceof Number)) return;

        String sql = "SELECT z FROM " + SCHEMA + "t_auto_hltgq_water_river_info " +
                     "WHERE stcd = ? AND z IS NOT NULL AND tm >= ? AND tm <= ? " +
                     "ORDER BY tm DESC LIMIT 1";
        Timestamp oneHourAgo = new Timestamp(System.currentTimeMillis() - 3600000);
        Timestamp twoHoursAgo = new Timestamp(System.currentTimeMillis() - 7200000);
        try {
            List<Double> results = jdbcTemplate.queryForList(sql, Double.class, stcd, twoHoursAgo, oneHourAgo);
            if (results != null && !results.isEmpty() && results.get(0) != null) {
                double rise = ((Number) zObj).doubleValue() - results.get(0);
                fieldMap.put("water_level_rise1h", trunc2(rise));
            }
        } catch (Exception e) {
            log.debug("计算1h水位涨幅失败, stcd={}: {}", stcd, e.getMessage());
        }
    }

    /**
     * 计算时段降雨量（mm）：rainfall1h/3h/6h = 当前DYP − N小时前DYP
     * <p>
     * DYP 是 RTU 安装以来的累计值，永不重置，差值即为时段降雨量。
     * DRP 每日 8:00 重置，不用于计算。
     */
    private void computeRainfall(Map<String, Object> fieldMap, JsonNode entity, String stcd, Set<String> validColumns) {
        if (!hasValue(entity, "DYP")) return;
        double currentDyp = entity.get("DYP").asDouble();

        if (isValidColumn(validColumns, "rainfall1h")) {
            Double prev = queryPreviousDyp(stcd, 3600000);
            if (prev != null) {
                double diff = trunc2(currentDyp - prev);
                if (diff >= 0) fieldMap.put("rainfall1h", diff);
            }
        }
        if (isValidColumn(validColumns, "rainfall3h")) {
            Double prev = queryPreviousDyp(stcd, 3 * 3600000);
            if (prev != null) {
                double diff = trunc2(currentDyp - prev);
                if (diff >= 0) fieldMap.put("rainfall3h", diff);
            }
        }
        if (isValidColumn(validColumns, "rainfall6h")) {
            Double prev = queryPreviousDyp(stcd, 6 * 3600000);
            if (prev != null) {
                double diff = trunc2(currentDyp - prev);
                if (diff >= 0) fieldMap.put("rainfall6h", diff);
            }
        }
    }

    /** 查询指定时间前的DYP累计值（DYP永不重置，差值始终≥0） */
    private Double queryPreviousDyp(String stcd, long intervalMs) {
        long lowerBoundMs = intervalMs * 2;
        String sql = "SELECT dyp FROM " + SCHEMA + "t_auto_hltgq_water_rain_info " +
                     "WHERE stcd = ? AND dyp IS NOT NULL AND tm >= ? AND tm <= ? " +
                     "ORDER BY tm DESC LIMIT 1";
        Timestamp from = new Timestamp(System.currentTimeMillis() - lowerBoundMs);
        Timestamp to   = new Timestamp(System.currentTimeMillis() - intervalMs);
        try {
            List<Double> results = jdbcTemplate.queryForList(sql, Double.class, stcd, from, to);
            if (results != null && !results.isEmpty() && results.get(0) != null) {
                return results.get(0);
            }
        } catch (Exception e) {
            log.debug("查询历史DYP失败, stcd={}: {}", stcd, e.getMessage());
        }
        return null;
    }

    /** 截断保留2位小数（超出直接舍弃，非四舍五入） */
    private static double trunc2(double v) {
        return (v >= 0 ? Math.floor(v * 100.0) : Math.ceil(v * 100.0)) / 100.0;
    }

    /**
     * 判断 entity 中指定字段是否有有效值（非 null、非 NullNode）
     */
    private boolean hasValue(JsonNode entity, String fieldName) {
        if (!entity.has(fieldName)) {
            return false;
        }
        JsonNode node = entity.get(fieldName);
        return node != null && !node.isNull();
    }

    /**
     * 将JsonNode值转为Java对象
     */
    private Object convertValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isInt()) {
            return node.asInt();
        }
        if (node.isLong()) {
            return node.asLong();
        }
        if (node.isDouble() || node.isFloat()) {
            return node.asDouble();
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        return node.asText();
    }

    // ======================== 站点状态 ========================

    /** 已标记在线的站点ID集合（当天），用于去重减少DB写 */
    private final Set<String> todayOnlineSet = ConcurrentHashMap.newKeySet();

    /**
     * 收到报文即标记站点在线（当天首次才写DB）
     */
    private void markSiteOnline(String siteId) {
        if (!todayOnlineSet.add(siteId)) return; // 当天已标记过
        try {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            String sql = "UPDATE " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef " +
                         "SET zebpsu = '#1#', updated_at = ?, updated_by = 'SYSTEM' " +
                         "WHERE id = ? AND (zebpsu IS NULL OR zebpsu != '#1#')";
            int rows = jdbcTemplate.update(sql, now, siteId);
            if (rows > 0) {
                log.debug("站点标记在线: site={}", siteId);
            }
        } catch (Exception e) {
            log.debug("标记站点在线失败, site={}: {}", siteId, e.getMessage());
        }
    }

    /**
     * 每小时检查：超过24小时未收到报文的站点标记为停用。
     * 收到过报文的站点 updated_at 会被 markSiteOnline 刷新，
     * 只有真正失联超过24h的才会被标记离线，程序异常不会误伤。
     */
    @Scheduled(fixedRate = 3600000)
    public void checkOfflineSites() {
        try {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            Timestamp cutoff = new Timestamp(now.getTime() - 86400000L); // 24h前
            String sql = "UPDATE " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef " +
                         "SET zebpsu = '#2#', updated_at = ?, updated_by = 'SYSTEM' " +
                         "WHERE zebpsu IS DISTINCT FROM '#2#' " +
                         "AND (updated_at IS NULL OR updated_at < ?)";
            int rows = jdbcTemplate.update(sql, now, cutoff);
            if (rows > 0) {
                log.info("标记离线站点: {} 个", rows);
            }
        } catch (Exception e) {
            log.error("离线站点检查失败", e);
        }
    }
}
