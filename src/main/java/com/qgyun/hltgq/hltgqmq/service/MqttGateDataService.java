package com.qgyun.hltgq.hltgqmq.service;

import com.qgyun.hltgq.hltgqmq.util.IdGenerator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * MQTT 闸门监测数据解析与入库服务
 *
 * 解析扁平 PLC 标签键值对，按站点前缀分组，
 * 提取水位(RX)、开度(R)、DI状态(B)，写入 t_auto_hltgq_water_gate 表
 */
@Service
public class MqttGateDataService {

    private static final Logger log = LoggerFactory.getLogger(MqttGateDataService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String SCHEMA = "\"qixiao-apaas\".";

    @Value("${app.corp-code:hltgq}")
    private String corpCode;

    private static final String GATE_TABLE   = SCHEMA + "t_auto_hltgq_water_gate";
    private static final String DEVICE_TABLE = SCHEMA + "t_auto_hltgq_water_device";

    /**
     * MQTT 前缀 → 站点/设备名称 (查 zzkaec 和 water_device.name 共用)
     * 设备表 name = 站点名 + 闸孔号 + "#"  例: "南山寺节制闸1#"
     */
    private static final Map<String, String> PREFIX_MAP = new LinkedHashMap<>();
    static {
        PREFIX_MAP.put("NSS",   "南山寺节制闸");
        PREFIX_MAP.put("QSDZ",  "渠首电站防洪闸");
        PREFIX_MAP.put("QSJSZ", "渠首进水闸");
        PREFIX_MAP.put("SMH",   "双庙湖节制闸");
    }

    /**
     * B 标签偏移 0~7 对应的数据库 DI 字段名
     */
    private static final String[] DI_FIELDS = {
        "local", "remote", "open_run", "close_run",
        "full_open", "full_close", "overload", "power_ok"
    };

    /** 闸门表有效列名缓存 */
    private Set<String> gateColumns = Collections.emptySet();

    /** 最新数据缓存: key = "siteId_deviceId_gateNo", value = fieldMap (会持续更新) */
    private final ConcurrentMap<String, Map<String, Object>> latestDataCache = new ConcurrentHashMap<>();

    /** site 名称 → siteId 缓存 (静态映射, 首次查DB后缓存) */
    private final ConcurrentMap<String, String> siteCache = new ConcurrentHashMap<>();

    /** device 名称 → deviceId 缓存 (首次查/建后缓存) */
    private final ConcurrentMap<String, String> deviceCache = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        try {
            String sql = "SELECT column_name FROM information_schema.columns " +
                    "WHERE table_schema = 'qixiao-apaas' AND table_name = 't_auto_hltgq_water_gate'";
            List<String> cols = jdbcTemplate.queryForList(sql, String.class);
            gateColumns = new HashSet<>();
            for (String col : cols) {
                gateColumns.add(col.toLowerCase());
            }
            log.info("已加载闸门表列名元数据: {} 列", gateColumns.size());
        } catch (Exception e) {
            log.error("加载闸门表列名元数据失败", e);
        }
    }

    /**
     * 处理一条 MQTT 报文，解析并更新内存缓存（不入库，由定时任务统一入库）
     */
    public void process(String payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            Timestamp now = new Timestamp(System.currentTimeMillis());

            Map<String, Map<String, String>> prefixData = groupByPrefix(root);

            int updated = 0;
            for (Map.Entry<String, Map<String, String>> entry : prefixData.entrySet()) {
                String prefix = entry.getKey();
                Map<String, String> fields = entry.getValue();

                String baseName = PREFIX_MAP.get(prefix);
                if (baseName == null) {
                    continue;
                }

                String siteId = lookupSiteByName(baseName);
                if (siteId == null) {
                    continue;
                }

                // MQTT报文也标记站点在线
                markSiteOnline(siteId);

                String upZ   = fields.get("RX_020");
                String downZ = fields.get("RX_021");

                Set<Integer> gateNos = new TreeSet<>();
                for (String key : fields.keySet()) {
                    if (key.startsWith("R_")) {
                        gateNos.add(Integer.parseInt(key.substring(2)) + 1);
                    } else if (key.startsWith("B_")) {
                        gateNos.add(Integer.parseInt(key.substring(2)) / 16 + 1);
                    }
                }

                for (Integer gateNo : gateNos) {
                    String deviceName = baseName + gateNo + "#";
                    String deviceId = lookupOrCreateDevice(deviceName, siteId);
                    if (deviceId == null) {
                        continue;
                    }

                    Map<String, Object> fieldMap = buildFieldMap(
                            siteId, deviceId, gateNo, fields, upZ, downZ, now);
                    // 缓存，key = siteId_deviceId_gateNo，每次覆盖最新值
                    String cacheKey = siteId + "_" + deviceId + "_" + gateNo;
                    latestDataCache.put(cacheKey, fieldMap);
                    updated++;
                }
            }

            log.debug("MQTT 数据已缓存, 共 {} 条", updated);

        } catch (Exception e) {
            log.error("MQTT 数据解析失败", e);
        }
    }

    /**
     * 定时入库：每 30 分钟将缓存中最新数据批量写入数据库
     */
    @Scheduled(fixedRate = 1800000)  // 30 分钟 = 1,800,000 ms
    public void flushToDb() {
        if (latestDataCache.isEmpty()) {
            log.debug("缓存为空，跳过入库");
            return;
        }

        // 取出并清空缓存
        List<Map<String, Object>> rows = new ArrayList<>(latestDataCache.values());
        latestDataCache.clear();

        // 更新每条记录的时间戳为当前时间
        Timestamp now = new Timestamp(System.currentTimeMillis());
        for (Map<String, Object> row : rows) {
            row.put("id", IdGenerator.generate());
            row.put("tm", now);
            row.put("date", now);
            row.put("ctime", now);
            row.put("created_at", now);
            row.put("updated_at", now);
        }

        // 构建列序（取所有行的 key 并集，避免跨站点字段不一致时丢失数据）
        Set<String> columnSet = new LinkedHashSet<>();
        for (Map<String, Object> row : rows) {
            for (String key : row.keySet()) {
                if (gateColumns.isEmpty() || gateColumns.contains(key.toLowerCase())) {
                    columnSet.add(key);
                }
            }
        }
        List<String> columns = new ArrayList<>(columnSet);
        // 固定列序，避免故障恢复时不同批次列序不一致导致错位
        Collections.sort(columns);

        StringBuilder colSb = new StringBuilder();
        StringBuilder placeholderSb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                colSb.append(", ");
                placeholderSb.append(", ");
            }
            colSb.append(columns.get(i));
            placeholderSb.append("?");
        }

        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                GATE_TABLE, colSb.toString(), placeholderSb.toString());

        try {
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    Map<String, Object> row = rows.get(i);
                    for (int j = 0; j < columns.size(); j++) {
                        ps.setObject(j + 1, row.get(columns.get(j)));
                    }
                }

                @Override
                public int getBatchSize() {
                    return rows.size();
                }
            });
            log.info("定时入库完成: {} 条记录 → {}", rows.size(), GATE_TABLE);
        } catch (Exception e) {
            log.error("定时入库失败, 数据丢失 {} 条", rows.size(), e);
            // 失败时写回缓存，下次再试
            for (Map<String, Object> row : rows) {
                String cacheKey = row.get("site") + "_" + row.get("device") + "_" + row.get("gate_no");
                latestDataCache.putIfAbsent(cacheKey, row);
            }
        }
    }

    // ======================== 解析 ========================

    /**
     * 将扁平 JSON 按前缀分组，并提取标签类型+编号
     *
     * 输入: {"NSS_HMI_B_000.PV":0, "NSS_HMI_R_000.PV":0.0004, ...}
     * 输出: {"NSS": {"B_000":"0", "R_000":"0.0004", "RX_020":"42.64", ...}, ...}
     */
    private Map<String, Map<String, String>> groupByPrefix(JsonNode root) {
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        Iterator<String> fieldNames = root.fieldNames();
        while (fieldNames.hasNext()) {
            String fullKey = fieldNames.next();
            int firstUnderscore = fullKey.indexOf('_');
            if (firstUnderscore <= 0) continue;

            String prefix = fullKey.substring(0, firstUnderscore);
            String suffix = fullKey.substring(firstUnderscore + 1); // "HMI_B_000.PV"

            // 去掉 "HMI_" 和 ".PV"
            if (suffix.startsWith("HMI_")) {
                suffix = suffix.substring(4);
            }
            if (suffix.endsWith(".PV")) {
                suffix = suffix.substring(0, suffix.length() - 3);
            }

            String value = root.get(fullKey).asText();
            result.computeIfAbsent(prefix, k -> new LinkedHashMap<>()).put(suffix, value);
        }
        return result;
    }

    // ======================== 查询 ========================

    /** 按站点名称查 t_auto_hltgq_5nw74_vnqqef.zzkaec → id (带缓存) */
    private String lookupSiteByName(String siteName) {
        return siteCache.computeIfAbsent(siteName, name -> {
            try {
                String sql = "SELECT id FROM " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef WHERE zzkaec = ?";
                List<String> results = jdbcTemplate.queryForList(sql, String.class, name);
                if (results != null && !results.isEmpty()) {
                    return results.get(0);
                }
            } catch (Exception e) {
                log.warn("查找站点失败, siteName={}, 错误: {}", name, e.getMessage());
            }
            return null;
        });
    }

    /** 按设备名查 t_auto_hltgq_water_device.name → id，不存在则自动创建 (带缓存) */
    private String lookupOrCreateDevice(String deviceName, String siteId) {
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
                log.info("已自动创建设备: name={}, id={}", name, deviceId);
                return deviceId;
            } catch (Exception e) {
                log.error("创建设备失败, name={}, 错误: {}", name, e.getMessage());
                return null;
            }
        });
    }

    // ======================== 组装字段 ========================

    private Map<String, Object> buildFieldMap(String siteId, String deviceId, int gateNo,
            Map<String, String> fields, String upZ, String downZ, Timestamp now) {

        Map<String, Object> map = new LinkedHashMap<>();

        // 系统字段（所有表统一）
        map.put("id",          IdGenerator.generate());
        map.put("corp_code",   corpCode);
        map.put("created_at",  now);
        map.put("created_by",  "SYSTEM");
        map.put("updated_at",  now);
        map.put("updated_by",  "SYSTEM");

        // 业务字段
        map.put("site",        siteId);
        map.put("device",      deviceId);
        map.put("gate_no",     String.valueOf(gateNo));
        map.put("tm",          now);
        map.put("date",        now);
        map.put("ctime",       now);
        map.put("status",      "#1#");

        // 水位（站级，每个闸孔行都填充）
        if (upZ != null) {
            Double val = parseDoubleSafe(upZ, "up_z");
            if (val != null) map.put("up_z", val);
        }
        if (downZ != null) {
            Double val = parseDoubleSafe(downZ, "down_z");
            if (val != null) map.put("down_z", val);
        }

        // 闸门开度 R_{gateNo-1} → open_degree
        String rKey = "R_" + String.format("%03d", gateNo - 1);
        if (fields.containsKey(rKey)) {
            Double val = parseDoubleSafe(fields.get(rKey), "open_degree");
            if (val != null) map.put("open_degree", val);
        }

        // DI 状态 B_{base}~B_{base+7} → local/remote/...
        int bBase = (gateNo - 1) * 16;
        for (int i = 0; i < 8; i++) {
            String bKey = "B_" + String.format("%03d", bBase + i);
            if (fields.containsKey(bKey)) {
                Integer val = parseIntSafe(fields.get(bKey), DI_FIELDS[i]);
                if (val != null) map.put(DI_FIELDS[i], val);
            }
        }

        return map;
    }

    /** 安全解析double，解析失败返回null并记录日志 */
    private Double parseDoubleSafe(String value, String fieldName) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            log.debug("MQTT数值解析失败, field={}, value={}", fieldName, value);
            return null;
        }
    }

    /** 安全解析int，解析失败返回null并记录日志 */
    private Integer parseIntSafe(String value, String fieldName) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.debug("MQTT数值解析失败, field={}, value={}", fieldName, value);
            return null;
        }
    }

    // ======================== 站点状态 ========================

    /** 已标记在线的站点ID集合（当天去重） */
    private final Set<String> todayOnlineSet = ConcurrentHashMap.newKeySet();

    /** 收到MQTT报文即标记站点在线（当天首次才写DB） */
    private void markSiteOnline(String siteId) {
        if (!todayOnlineSet.add(siteId)) return;
        try {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            // 无条件更新：跨天后即使已经 #1# 也要刷新 updated_at，
            // 否则 checkOfflineSites 会在 24h 后误标为离线
            String sql = "UPDATE " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef " +
                         "SET zebpsu = '#1#', updated_at = ?, updated_by = 'SYSTEM' " +
                         "WHERE id = ?";
            int rows = jdbcTemplate.update(sql, now, siteId);
            if (rows > 0) {
                log.debug("MQTT站点标记在线: site={}", siteId);
            }
        } catch (Exception e) {
            log.debug("MQTT标记站点在线失败, site={}: {}", siteId, e.getMessage());
        }
    }

    /** 每天0点清空 todayOnlineSet，确保失联后恢复的站点能被重新标记在线 */
    @Scheduled(cron = "0 0 0 * * ?")
    public void resetOnlineSet() {
        int size = todayOnlineSet.size();
        todayOnlineSet.clear();
        log.debug("MQTT todayOnlineSet已重置, {} 个站点可重新标记在线", size);
    }

}
