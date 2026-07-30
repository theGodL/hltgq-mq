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

    /** 站点类型(epjutj) → 设备名称后缀 映射 */
    private static final Map<String, String> SITE_TYPE_DEVICE_MAP = new LinkedHashMap<>();
    static {
        SITE_TYPE_DEVICE_MAP.put("1", "水位计");     // 水位站
        SITE_TYPE_DEVICE_MAP.put("2", "雨量计");     // 雨量站
        SITE_TYPE_DEVICE_MAP.put("3", "流量计");     // 流量站
        SITE_TYPE_DEVICE_MAP.put("6", "泵站");       // 泵站
        SITE_TYPE_DEVICE_MAP.put("8", "水质监测仪"); // 水质
    }

    /**
     * 表列名缓存：表名(不含schema) -> 该表所有列名的集合
     */
    private final Map<String, Set<String>> tableColumnsCache = new HashMap<>();

    /** 设备名 → device_id 缓存（首次查/建后缓存） */
    private final ConcurrentMap<String, String> deviceCache = new ConcurrentHashMap<>();

    /** stcd → 主设备 device_id 缓存（闸站=闸孔1，非闸站=RTU设备） */
    private final ConcurrentMap<String, String> stcdDeviceCache = new ConcurrentHashMap<>();

    /** site_id → 站点名称 缓存 */
    private final ConcurrentMap<String, String> siteNameCache = new ConcurrentHashMap<>();

    /** site_id → epjutj(站点类型) 缓存 */
    private final ConcurrentMap<String, String> siteTypeCache = new ConcurrentHashMap<>();

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
            if (isValidColumn(validColumns, "device")) {
                String device = lookupOrCreateRtuDevice(stcd, site);
                if (device == null) {
                    log.error("设备创建失败, 丢弃报文: stcd={}, tag={}", stcd, tag);
                    return;
                }
                fieldMap.put("device", device);
            }
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
            // === msgInfo 条件入库：msg为空或空白则跳过 ===
            if ("msgInfo".equals(tag)) {
                if (!hasValue(entity, "MSG") || entity.get("MSG").asText().trim().isEmpty()) {
                    log.debug("msgInfo 报文(MSG)为空, 跳过入库, stcd={}", stcd);
                    return;
                }
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
            // === rainInfo 条件入库：DYP≤0说明设备无雨量监测能力或报文异常，跳过入库 ===
            // DRP可为0（今日无雨），DYP是RTU安装以来累计值，为0则设备不匹配
            if ("rainInfo".equals(tag)) {
                if (!hasValue(entity, "DYP") || entity.get("DYP").asDouble() <= 0) {
                    log.debug("rainInfo 无有效雨量数据(DYP缺失或≤0), 跳过入库, stcd={}", stcd);
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

            // 去重：相同 stcd+tm 已存在则跳过（防止RabbitMQ重投产生重复行）
            if (fieldMap.containsKey("tm") && fieldMap.get("tm") != null) {
                try {
                    String checkSql = "SELECT COUNT(*) FROM " + tableName + " WHERE stcd = ? AND tm = ?";
                    int count = jdbcTemplate.queryForObject(checkSql, Integer.class, stcd, fieldMap.get("tm"));
                    if (count > 0) {
                        log.debug("报文重复, 跳过入库: tag={}, stcd={}, tm={}", tag, stcd, fieldMap.get("tm"));
                        return;
                    }
                } catch (Exception e) {
                    log.debug("去重检查失败, 放行入库: {}", e.getMessage());
                }
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
     * 按站点类型查找或创建设备，返回 device_id (带缓存)
     * <p>
     * 闸站(#4#): 生成闸孔1设备 "{siteName}闸孔1#"，所有站级数据指向它
     * 非闸站: 生成RTU设备 "{siteName}{类型后缀}#"
     */
    private String lookupOrCreateRtuDevice(String stcd, String siteId) {
        return stcdDeviceCache.computeIfAbsent(stcd, s -> {
            String siteName = getSiteName(siteId);
            String epjutj = getSiteType(siteId);
            String deviceName;
            String deviceType;

            // 闸站(#4#): 统一用闸孔1设备
            if (epjutj != null && epjutj.contains("#4#")) {
                deviceName = siteName + "1#";
                deviceType = "#4#";
            } else {
                // 非闸站: 按站点类型生成 RTU 设备
                String typeCode = extractPrimarySiteType(epjutj);
                String suffix = SITE_TYPE_DEVICE_MAP.getOrDefault(typeCode, "遥测终端");
                deviceName = siteName + suffix + "#";
                deviceType = epjutj;
            }
            return lookupOrCreateDeviceByName(deviceName, siteId, deviceType);
        });
    }

    /** 按设备名称查找或创建（共享缓存），可选写入 type 字段 */
    private String lookupOrCreateDeviceByName(String deviceName, String siteId) {
        return lookupOrCreateDeviceByName(deviceName, siteId, null);
    }

    private String lookupOrCreateDeviceByName(String deviceName, String siteId, String type) {
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
                String sql;
                if (type != null) {
                    sql = "INSERT INTO " + DEVICE_TABLE +
                          " (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    jdbcTemplate.update(sql, deviceId, corpCode, now, "SYSTEM", now, "SYSTEM", name, siteId, type);
                } else {
                    sql = "INSERT INTO " + DEVICE_TABLE +
                          " (id, corp_code, created_at, created_by, updated_at, updated_by, name, site) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                    jdbcTemplate.update(sql, deviceId, corpCode, now, "SYSTEM", now, "SYSTEM", name, siteId);
                }
                log.info("已自动创建设备: name={}, id={}, type={}", name, deviceId, type);
                return deviceId;
            } catch (Exception e) {
                log.error("创建设备失败, name={}, 错误: {}", name, e.getMessage());
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

    /** 根据 site_id 查站点类型 epjutj（带缓存），如 "#4#" */
    private String getSiteType(String siteId) {
        return siteTypeCache.computeIfAbsent(siteId, id -> {
            try {
                String sql = "SELECT epjutj FROM " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef WHERE id = ?";
                List<String> results = jdbcTemplate.queryForList(sql, String.class, id);
                if (results != null && !results.isEmpty() && results.get(0) != null) {
                    return results.get(0);
                }
            } catch (Exception e) {
                log.warn("查找站点类型失败, siteId={}, 错误: {}", id, e.getMessage());
            }
            return null;
        });
    }

    /** 从 epjutj 提取主类型码，如 "#4#" → "4"，"#1#|#3#" → "1" */
    private String extractPrimarySiteType(String epjutj) {
        if (epjutj == null || epjutj.isEmpty()) return null;
        int s = epjutj.indexOf('#');
        if (s < 0) return null;
        int e = epjutj.indexOf('#', s + 1);
        if (e < 0) return null;
        return epjutj.substring(s + 1, e);
    }

    /**
     * riverInfo 闸站路由：优先更新已有闸孔行（gate_no≠0）的 up_z/down_z，
     * 无已有行时才写入 gate_no=0 占位（等 gatesInfo 到达后自动合并）。
     */
    private void insertGateFromRiverInfo(JsonNode entity, String siteId, String stcd) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Set<String> gateCols = tableColumnsCache.getOrDefault("t_auto_hltgq_water_gate", Collections.emptySet());

        // 使用报文 TM，兜底服务器时间
        Timestamp tm = extractTm(entity, now);

        String device = lookupOrCreateRtuDevice(stcd, siteId);
        if (device == null) {
            log.error("riverInfo→gate 设备缺失, 跳过入库: stcd={}", stcd);
            return;
        }

        double z1 = hasValue(entity, "Z1") && entity.get("Z1").asDouble() > 0 ? entity.get("Z1").asDouble() : -1;
        double z2 = hasValue(entity, "Z2") && entity.get("Z2").asDouble() > 0 ? entity.get("Z2").asDouble() : -1;
        if (z1 < 0 && z2 < 0) return;

        // 优先尝试更新已有闸孔行（gatesInfo 可能已先到达）
        Timestamp recentWindow = new Timestamp(tm.getTime() - 600000); // 10分钟窗口
        StringBuilder updateSql = new StringBuilder("UPDATE " + GATE_TABLE + " SET updated_at = ?, updated_by = 'SYSTEM'");
        List<Object> updateParams = new ArrayList<>();
        updateParams.add(now);
        if (z1 >= 0) {
            updateSql.append(", up_z = ?");
            updateParams.add(z1);
        }
        if (z2 >= 0) {
            updateSql.append(", down_z = ?");
            updateParams.add(z2);
        }
        updateSql.append(" WHERE stcd = ? AND tm >= ? AND gate_no != '0'");
        updateParams.add(stcd);
        updateParams.add(recentWindow);

        int updated = jdbcTemplate.update(updateSql.toString(), updateParams.toArray());
        if (updated > 0) {
            log.info("riverInfo水位已合并到 {} 条闸孔行: stcd={}, Z1={}, Z2={}", updated, stcd,
                     z1 >= 0 ? z1 : "null", z2 >= 0 ? z2 : "null");
            return;
        }

        // 去重：相同 stcd+tm+gate_no=0 已存在则跳过（防止RabbitMQ重投）
        try {
            String checkSql = "SELECT COUNT(*) FROM " + GATE_TABLE + " WHERE stcd = ? AND tm = ? AND gate_no = '0'";
            int count = jdbcTemplate.queryForObject(checkSql, Integer.class, stcd, tm);
            if (count > 0) {
                log.debug("gate_no=0占位行已存在, 跳过: stcd={}, tm={}", stcd, tm);
                return;
            }
        } catch (Exception e) {
            log.debug("gate表去重检查失败, 放行: {}", e.getMessage());
        }

        // 无已有闸孔行可更新 → 写入 gate_no=0 占位（等 gatesInfo 到达后合并）
        Map<String, Object> fm = new LinkedHashMap<>();
        fm.put("id",         IdGenerator.generate());
        fm.put("corp_code",  corpCode);
        fm.put("created_at", now);
        fm.put("created_by", "SYSTEM");
        fm.put("updated_at", now);
        fm.put("updated_by", "SYSTEM");
        if (gateCols.isEmpty() || gateCols.contains("site"))     fm.put("site",     siteId);
        if (gateCols.isEmpty() || gateCols.contains("device"))   fm.put("device",   device);
        if (gateCols.isEmpty() || gateCols.contains("stcd"))     fm.put("stcd",     stcd);
        if (gateCols.isEmpty() || gateCols.contains("gate_no"))  fm.put("gate_no",  "0");
        if (gateCols.isEmpty() || gateCols.contains("tm"))       fm.put("tm",       tm);
        if (gateCols.isEmpty() || gateCols.contains("date"))     fm.put("date",     tm);
        if (gateCols.isEmpty() || gateCols.contains("ctime"))    fm.put("ctime",    tm);
        if (gateCols.isEmpty() || gateCols.contains("status"))   fm.put("status",   "#1#");
        if (z1 >= 0) fm.put("up_z", z1);
        if (z2 >= 0) fm.put("down_z", z2);

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

        log.info("riverInfo闸站数据已写入gate_no=0占位: stcd={}, Z1={}, Z2={}", stcd,
                 z1 >= 0 ? z1 : "null", z2 >= 0 ? z2 : "null");
    }

    /**
     * gatesInfo 闸门开度路由：将 Gates1/2/3 → gate 表多行（闸孔级开度，gate_no=1/2/3）
     * 每闸孔独立设备，命名: {站点名}{闸孔号}#
     * 写入完成后自动合并 pending 的 gate_no=0 水位占位行。
     */
    private void insertGateFromGatesInfo(JsonNode entity, String siteId, String stcd) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Set<String> gateCols = tableColumnsCache.getOrDefault("t_auto_hltgq_water_gate", Collections.emptySet());
        String siteName = getSiteName(siteId);

        // 使用报文 TM，兜底服务器时间
        Timestamp tm = extractTm(entity, now);

        int inserted = 0;
        for (int i = 1; i <= 10; i++) {
            String fieldName = "Gates" + i;
            if (!hasValue(entity, fieldName)) continue; // 跳过null字段，继续处理后续闸孔

            double openDegree = entity.get(fieldName).asDouble();
            // -999 表示无信号，跳过该闸孔（不创建设备）
            if (openDegree == -999) continue;

            String deviceName = siteName + i + "#";
            String deviceId = lookupOrCreateDeviceByName(deviceName, siteId);
            if (deviceId == null) {
                log.error("gatesInfo 设备缺失, 跳过闸孔{}: stcd={}, deviceName={}", i, stcd, deviceName);
                continue;
            }

            // 去重：相同 stcd+tm+gate_no 已存在则跳过（防止RabbitMQ重投）
            try {
                String checkSql = "SELECT COUNT(*) FROM " + GATE_TABLE + " WHERE stcd = ? AND tm = ? AND gate_no = ?";
                int count = jdbcTemplate.queryForObject(checkSql, Integer.class, stcd, tm, String.valueOf(i));
                if (count > 0) {
                    log.debug("gate表重复, 跳过闸孔{}: stcd={}, tm={}", i, stcd, tm);
                    continue;
                }
            } catch (Exception e) {
                log.debug("gate表去重检查失败, 放行: {}", e.getMessage());
            }

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
            if (gateCols.isEmpty() || gateCols.contains("tm"))          fm.put("tm",           tm);
            if (gateCols.isEmpty() || gateCols.contains("date"))        fm.put("date",         tm);
            if (gateCols.isEmpty() || gateCols.contains("ctime"))       fm.put("ctime",        tm);
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

        // 合并 pending 的 gate_no=0 水位占位行（riverInfo 可能已先到达）
        if (inserted > 0) {
            mergeGateZeroWaterLevels(stcd, tm, now);
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

        String device = fieldMap.containsKey("device") ? (String) fieldMap.get("device") : null;
        Timestamp oneHourAgo = new Timestamp(System.currentTimeMillis() - 3600000);
        Timestamp twoHoursAgo = new Timestamp(System.currentTimeMillis() - 7200000);
        try {
            List<Double> results;
            if (device != null) {
                String sql = "SELECT z FROM " + SCHEMA + "t_auto_hltgq_water_river_info " +
                             "WHERE stcd = ? AND device = ? AND z IS NOT NULL AND tm >= ? AND tm <= ? " +
                             "ORDER BY tm DESC LIMIT 1";
                results = jdbcTemplate.queryForList(sql, Double.class, stcd, device, twoHoursAgo, oneHourAgo);
            } else {
                String sql = "SELECT z FROM " + SCHEMA + "t_auto_hltgq_water_river_info " +
                             "WHERE stcd = ? AND z IS NOT NULL AND tm >= ? AND tm <= ? " +
                             "ORDER BY tm DESC LIMIT 1";
                results = jdbcTemplate.queryForList(sql, Double.class, stcd, twoHoursAgo, oneHourAgo);
            }
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

        String device = fieldMap.containsKey("device") ? (String) fieldMap.get("device") : null;

        if (isValidColumn(validColumns, "rainfall1h")) {
            Double prev = queryPreviousDyp(stcd, device, 3600000);
            if (prev != null) {
                double diff = trunc2(currentDyp - prev);
                if (diff >= 0) {
                    fieldMap.put("rainfall1h", diff);
                } else {
                    log.debug("rainfall1h计算异常(DYP回退), stcd={}, currentDyp={}, prevDyp={}, diff={}", stcd, currentDyp, prev, diff);
                }
            }
        }
        if (isValidColumn(validColumns, "rainfall3h")) {
            Double prev = queryPreviousDyp(stcd, device, 3 * 3600000);
            if (prev != null) {
                double diff = trunc2(currentDyp - prev);
                if (diff >= 0) {
                    fieldMap.put("rainfall3h", diff);
                } else {
                    log.debug("rainfall3h计算异常(DYP回退), stcd={}, currentDyp={}, prevDyp={}, diff={}", stcd, currentDyp, prev, diff);
                }
            }
        }
        if (isValidColumn(validColumns, "rainfall6h")) {
            Double prev = queryPreviousDyp(stcd, device, 6 * 3600000);
            if (prev != null) {
                double diff = trunc2(currentDyp - prev);
                if (diff >= 0) {
                    fieldMap.put("rainfall6h", diff);
                } else {
                    log.debug("rainfall6h计算异常(DYP回退), stcd={}, currentDyp={}, prevDyp={}, diff={}", stcd, currentDyp, prev, diff);
                }
            }
        }
    }

    /** 查询指定时间前的DYP累计值（DYP永不重置，差值始终≥0） */
    private Double queryPreviousDyp(String stcd, String device, long intervalMs) {
        long lowerBoundMs = intervalMs * 2;
        Timestamp from = new Timestamp(System.currentTimeMillis() - lowerBoundMs);
        Timestamp to   = new Timestamp(System.currentTimeMillis() - intervalMs);
        try {
            List<Double> results;
            if (device != null) {
                String sql = "SELECT dyp FROM " + SCHEMA + "t_auto_hltgq_water_rain_info " +
                             "WHERE stcd = ? AND device = ? AND dyp IS NOT NULL AND tm >= ? AND tm <= ? " +
                             "ORDER BY tm DESC LIMIT 1";
                results = jdbcTemplate.queryForList(sql, Double.class, stcd, device, from, to);
            } else {
                String sql = "SELECT dyp FROM " + SCHEMA + "t_auto_hltgq_water_rain_info " +
                             "WHERE stcd = ? AND dyp IS NOT NULL AND tm >= ? AND tm <= ? " +
                             "ORDER BY tm DESC LIMIT 1";
                results = jdbcTemplate.queryForList(sql, Double.class, stcd, from, to);
            }
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

    // ======================== Gate 辅助方法 ========================

    /** 从 entity 中提取测量时间 TM，解析失败则用兜底时间 */
    private Timestamp extractTm(JsonNode entity, Timestamp fallback) {
        if (!hasValue(entity, "TM")) return fallback;
        try {
            JsonNode tmNode = entity.get("TM");
            if (tmNode.isNumber()) {
                return new Timestamp(tmNode.asLong());
            }
            return Timestamp.valueOf(tmNode.asText());
        } catch (Exception e) {
            log.debug("无法解析TM字段, 使用兜底时间: {}", entity.get("TM"));
            return fallback;
        }
    }

    /** 将 gate_no=0 占位行的水位合并到闸孔行，并清理占位行 */
    private void mergeGateZeroWaterLevels(String stcd, Timestamp tm, Timestamp now) {
        try {
            Timestamp recentWindow = new Timestamp(tm.getTime() - 600000); // 10分钟窗口
            String selectSql = "SELECT up_z, down_z FROM " + GATE_TABLE +
                    " WHERE stcd = ? AND gate_no = '0' AND tm >= ? ORDER BY tm DESC LIMIT 1";
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(selectSql, stcd, recentWindow);
            if (rows.isEmpty()) return;

            Map<String, Object> pending = rows.get(0);
            Object upZ = pending.get("up_z");
            Object downZ = pending.get("down_z");
            if (upZ == null && downZ == null) return;

            StringBuilder updateSql = new StringBuilder("UPDATE " + GATE_TABLE + " SET updated_at = ?, updated_by = 'SYSTEM'");
            List<Object> updateParams = new ArrayList<>();
            updateParams.add(now);
            if (upZ != null) {
                updateSql.append(", up_z = ?");
                updateParams.add(upZ);
            }
            if (downZ != null) {
                updateSql.append(", down_z = ?");
                updateParams.add(downZ);
            }
            updateSql.append(" WHERE stcd = ? AND tm >= ? AND gate_no != '0'");
            updateParams.add(stcd);
            updateParams.add(recentWindow);

            int merged = jdbcTemplate.update(updateSql.toString(), updateParams.toArray());

            // 清理 gate_no=0 占位行
            String deleteSql = "DELETE FROM " + GATE_TABLE + " WHERE stcd = ? AND gate_no = '0' AND tm >= ?";
            int deleted = jdbcTemplate.update(deleteSql, stcd, recentWindow);

            log.info("已合并gate_no=0水位到{}条闸孔行, 清理{}条占位: stcd={}", merged, deleted, stcd);
        } catch (Exception e) {
            log.warn("合并gate_no=0水位数据失败, stcd={}: {}", stcd, e.getMessage());
        }
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

    /**
     * 每天0点清空 todayOnlineSet，确保失联后恢复的站点能被重新标记在线
     */
    @Scheduled(cron = "0 0 0 * * ?")
    public void resetOnlineSet() {
        int size = todayOnlineSet.size();
        todayOnlineSet.clear();
        log.debug("todayOnlineSet已重置, {} 个站点可重新标记在线", size);
    }
}
