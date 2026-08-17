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
    private static final String WT_NFO_TABLE = SCHEMA + "t_auto_hltgq_water_wt_nfo";
    private static final String SLUICE_TABLE = SCHEMA + "t_auto_hltgq_water_sluice_discharge";

    /** ===== 数值守卫边界（与 RabbitMQ 路径一致，审计定版） ===== */
    /** 水位上限(m)，河道/闸站水位基准差异大，取宽松值 */
    private static final double MAX_WATER_LEVEL = 1000;
    /** 闸门开度上限(m)，弧形/平面闸门物理开度远小于此值 */
    private static final double MAX_OPEN_DEGREE = 50;
    /** FFFFFFFF(4294967295) 传感器通讯异常哨兵值 */
    private static final double SENSOR_COMM_ERR = 4294967295.0;

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

    /** wt_nfo 流量表有效列名缓存 */
    private Set<String> wtColumns = Collections.emptySet();

    /** 最新数据缓存: key = "siteId_deviceId_gateNo", value = fieldMap (会持续更新) */
    private final ConcurrentMap<String, Map<String, Object>> latestDataCache = new ConcurrentHashMap<>();

    /** site 名称 → siteId 缓存 (静态映射, 首次查DB后缓存) */
    private final ConcurrentMap<String, String> siteCache = new ConcurrentHashMap<>();

    /** device 名称 → deviceId 缓存 (首次查/建后缓存) */
    private final ConcurrentMap<String, String> deviceCache = new ConcurrentHashMap<>();

    /** site → 流量计算配置缓存（5分钟过期，页面编辑配置 version+1 后自动生效） */
    private final ConcurrentMap<String, SluiceConfigEntry> sluiceConfigCache = new ConcurrentHashMap<>();

    /** 流量计算配置缓存有效期（5 分钟） */
    private static final long SLUICE_CACHE_TTL_MS = 5 * 60 * 1000L;

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
        try {
            String sql = "SELECT column_name FROM information_schema.columns " +
                    "WHERE table_schema = 'qixiao-apaas' AND table_name = 't_auto_hltgq_water_wt_nfo'";
            List<String> cols = jdbcTemplate.queryForList(sql, String.class);
            wtColumns = new HashSet<>();
            for (String col : cols) {
                wtColumns.add(col.toLowerCase());
            }
            log.info("已加载流量表列名元数据: {} 列", wtColumns.size());
        } catch (Exception e) {
            log.error("加载流量表列名元数据失败", e);
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
                    try {
                        if (key.startsWith("R_")) {
                            gateNos.add(Integer.parseInt(key.substring(2)) + 1);
                        } else if (key.startsWith("B_")) {
                            gateNos.add(Integer.parseInt(key.substring(2)) / 16 + 1);
                        }
                    } catch (NumberFormatException e) {
                        // 非规范标签(如R_XXX)跳过，不因单键异常丢弃整条报文
                        log.debug("MQTT闸孔标签编号解析失败, key={}", key);
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
     * 定时入库：服务器整点/半点(如16:00:00、16:30:00)将缓存中最新数据批量写入数据库
     */
    @Scheduled(cron = "0 0,30 * * * ?")  // 每小时 0 分和 30 分执行，墙钟对齐服务器时区
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

        // 去重：site+device+gate_no+tm 已存在则跳过（防止批量写入部分成功后的重试产生重复行）
        rows.removeIf(row -> {
            try {
                String checkSql = "SELECT COUNT(*) FROM " + GATE_TABLE +
                        " WHERE site = ? AND device = ? AND gate_no = ? AND tm = ?";
                Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class,
                        row.get("site"), row.get("device"), row.get("gate_no"), row.get("tm"));
                return count != null && count > 0;
            } catch (Exception e) {
                log.debug("MQTT去重检查失败, 放行: {}", e.getMessage());
                return false;
            }
        });
        if (rows.isEmpty()) {
            log.debug("缓存行均已入库, 跳过本批");
            return;
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
            return;
        }

        // 闸门数据入库成功后，计算各站实时流量并写入流量表（失败仅跳过本批流量，不影响闸门数据）
        try {
            List<Map<String, Object>> wtRows = buildWtRows(rows);
            insertWtRows(wtRows);
        } catch (Exception e) {
            log.warn("闸站流量计算/入库失败, 跳过本批流量写入", e);
        }
    }

    // ======================== 流量计算 ========================

    /**
     * 聚合本批缓存行，按站计算实时流量，生成 wt_nfo 流量表行。
     * 守卫场景（水位/开度缺失、无配置、倒流、H≤0 等）跳过该站。
     */
    private List<Map<String, Object>> buildWtRows(List<Map<String, Object>> rows) {
        // 按 site 聚合（水位是站级共享值，开度是孔级）
        Map<String, List<Map<String, Object>>> bySite = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            Object site = row.get("site");
            if (site == null) {
                continue;
            }
            bySite.computeIfAbsent(String.valueOf(site), k -> new ArrayList<>()).add(row);
        }

        List<Map<String, Object>> wtRows = new ArrayList<>();
        for (Map.Entry<String, List<Map<String, Object>>> entry : bySite.entrySet()) {
            String siteId = entry.getKey();
            List<Map<String, Object>> siteRows = entry.getValue();

            // 守卫5/6：无配置或脏配置 → 整站跳过
            GateDischargeCalculator.SluiceConfig config = getSluiceConfig(siteId);
            if (config == null) {
                log.warn("站点无有效流量计算配置, 跳过流量入库: site={}", siteId);
                continue;
            }

            // 水位为站级共享值，取该站任一行
            Map<String, Object> sampleRow = siteRows.get(0);
            Double upZ = toDouble(sampleRow.get("up_z"));
            if (upZ == null || upZ.isNaN()) {
                log.debug("上游水位缺失, 跳过流量计算: site={}", siteId);
                continue;
            }
            Double downZ = toDouble(sampleRow.get("down_z"));
            if (downZ == null || downZ.isNaN()) {
                log.debug("下游水位缺失, 跳过流量计算: site={}", siteId);
                continue;
            }

            // 逐孔收集开度；首孔设备取 gate_no 最小的行（站级流量归首孔，与 RabbitMQ 一致）
            List<Double> openDegrees = new ArrayList<>();
            Map<String, Object> firstGateRow = null;
            int firstGateNo = Integer.MAX_VALUE;
            boolean skip = false;
            for (Map<String, Object> row : siteRows) {
                Double openDegree = toDouble(row.get("open_degree"));
                if (openDegree == null || openDegree.isNaN() || openDegree < 0) {
                    // 守卫4：任一孔开度缺失或为负 → 整站跳过（避免站流量偏小失真）
                    log.debug("闸孔开度缺失或异常, 整站跳过流量计算: site={}, gate_no={}",
                            siteId, row.get("gate_no"));
                    skip = true;
                    break;
                }
                openDegrees.add(openDegree);
                int gateNo = parseInt(row.get("gate_no"));
                if (gateNo < firstGateNo) {
                    firstGateNo = gateNo;
                    firstGateRow = row;
                }
            }
            if (skip || openDegrees.isEmpty()) {
                continue;
            }

            Double totalQ = GateDischargeCalculator.calculateSiteDischarge(
                    config, upZ, downZ, openDegrees, siteId);
            if (totalQ == null) {
                continue;
            }

            // 日累计流量：梯形积分(Q×Δt)累加到当天0点起的日累计，随行入库 tf 字段
            Timestamp wtNow = new Timestamp(System.currentTimeMillis());
            double dailyTf = computeDailyTf(siteId, wtNow, totalQ);
            wtRows.add(buildWtRow(siteId, firstGateRow != null ? firstGateRow : sampleRow,
                    upZ, totalQ, wtNow, dailyTf));
        }
        return wtRows;
    }

    /**
     * 组装 wt_nfo 站级一行：stcd 不写（MQTT 无 stcd），site + device 关联，z=上游水位，q=总流量，tf=日累计流量
     */
    private Map<String, Object> buildWtRow(String siteId, Map<String, Object> gateRow,
                                           double upZ, double totalQ, Timestamp tm, double tf) {
        Map<String, Object> map = new LinkedHashMap<>();

        // 系统字段（与 RabbitMQ 行结构一致）
        map.put("id", IdGenerator.generate());
        map.put("corp_code", corpCode);
        map.put("created_at", tm);
        map.put("created_by", "SYSTEM");
        map.put("updated_at", tm);
        map.put("updated_by", "SYSTEM");

        // 业务字段（按 wt_nfo 实际列过滤，列名元数据缺失时不校验直接写入）
        if (wtColumns.isEmpty() || wtColumns.contains("site")) {
            map.put("site", siteId);
        }
        if (wtColumns.isEmpty() || wtColumns.contains("device")) {
            Object device = gateRow.get("device");
            if (device != null) {
                map.put("device", device);
            }
        }
        if (wtColumns.isEmpty() || wtColumns.contains("z")) {
            map.put("z", upZ);
        }
        if (wtColumns.isEmpty() || wtColumns.contains("q")) {
            map.put("q", totalQ);
        }
        if (wtColumns.isEmpty() || wtColumns.contains("tm")) {
            map.put("tm", tm);
        }
        if (wtColumns.isEmpty() || wtColumns.contains("tf")) {
            map.put("tf", tf);
        }
        return map;
    }

    /**
     * 计算站点日累计流量（当天0点起，梯形积分）：
     * 查 wt_nfo 该站最近一行(MQTT行)，增量 = (lastQ + q) / 2 × Δt。
     * 同一天：继承上行星 tf、以上行 tm 为积分起点；
     * 跨天/首行：日累计归零，从当天 0 点起算（跨天首行仍用昨天 Q 做梯形底）。
     * 间隔超 2h 视为数据断层(断报/停机)，该段不积分防止日累计虚增；
     * 1~2h 缺批容忍并积分(近似值)但 WARN 提醒排查。
     */
    private double computeDailyTf(String siteId, Timestamp now, double q) {
        try {
            Timestamp dayStart = Timestamp.valueOf(now.toLocalDateTime().toLocalDate().atStartOfDay());
            double baseTf = 0;
            double lastQ = q;
            Timestamp effectiveStart = dayStart;

            String sql = "SELECT tm, q, tf FROM " + WT_NFO_TABLE +
                    " WHERE site = ? AND (stcd IS NULL OR stcd = '') AND tm < ? ORDER BY tm DESC LIMIT 1";
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, siteId, now);
            if (!rows.isEmpty()) {
                Map<String, Object> row = rows.get(0);
                Timestamp lastTm = toDbTimestamp(row.get("tm"));
                Double lastQVal = toDouble(row.get("q"));
                Double lastTfVal = toDouble(row.get("tf"));
                if (lastTm != null && lastTm.getTime() >= dayStart.getTime() && lastQVal != null) {
                    // 同一天：继承日累计，以上行 tm 为积分起点
                    lastQ = lastQVal;
                    effectiveStart = lastTm;
                    if (lastTfVal != null) {
                        baseTf = lastTfVal;
                    }
                }
                // 跨天/上行无效：effectiveStart 保持 dayStart，baseTf=0 → 今天重新累计
            }

            long dtMs = now.getTime() - effectiveStart.getTime();
            if (dtMs < 0) {
                dtMs = 0;
            }
            double dtSec = dtMs / 1000.0;
            double delta = 0;
            if (dtSec > 7200) {
                // 断层超2h：期间的流量曲线无从知晓，宁缺毋滥，不积分
                log.warn("MQTT流量积分间隔超2h(断报/停机), 该段不积分: site={}, 间隔={}s", siteId, dtSec);
            } else {
                if (dtSec > 3600) {
                    log.warn("MQTT流量积分间隔超1h(疑似缺批), 积分为近似值: site={}, 间隔={}s", siteId, dtSec);
                }
                delta = (lastQ + q) / 2.0 * dtSec;
            }
            return baseTf + delta;
        } catch (Exception e) {
            log.warn("计算日累计流量失败, tf不入库: site={}, {}", siteId, e.getMessage());
            return 0;
        }
    }

    /** Object → Timestamp（兼容 Timestamp/Date/文本），转换失败返回 null */
    private Timestamp toDbTimestamp(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        try {
            return Timestamp.valueOf(String.valueOf(value));
        } catch (Exception e) {
            return null;
        }
    }

    /** 批量写入流量表（列序固定，与闸门表写入方式一致） */
    private void insertWtRows(List<Map<String, Object>> wtRows) {
        if (wtRows.isEmpty()) {
            return;
        }

        Set<String> columnSet = new LinkedHashSet<>();
        for (Map<String, Object> row : wtRows) {
            for (String key : row.keySet()) {
                if (wtColumns.isEmpty() || wtColumns.contains(key.toLowerCase())) {
                    columnSet.add(key);
                }
            }
        }
        List<String> columns = new ArrayList<>(columnSet);
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
                WT_NFO_TABLE, colSb.toString(), placeholderSb.toString());

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, Object> row = wtRows.get(i);
                for (int j = 0; j < columns.size(); j++) {
                    ps.setObject(j + 1, row.get(columns.get(j)));
                }
            }

            @Override
            public int getBatchSize() {
                return wtRows.size();
            }
        });
        log.info("闸站流量入库完成: {} 条记录 → {}", wtRows.size(), WT_NFO_TABLE);
    }

    /**
     * 查询站点流量计算配置：version 最大 + created_at 最新，带 5 分钟缓存。
     * 查无记录/脏配置返回 null，同样缓存（避免每批反复查库）。
     */
    private GateDischargeCalculator.SluiceConfig getSluiceConfig(String siteId) {
        long now = System.currentTimeMillis();
        SluiceConfigEntry cached = sluiceConfigCache.get(siteId);
        if (cached != null && now - cached.loadTime < SLUICE_CACHE_TTL_MS) {
            return cached.config;
        }

        GateDischargeCalculator.SluiceConfig config = loadSluiceConfig(siteId);
        sluiceConfigCache.put(siteId, new SluiceConfigEntry(config, now));
        return config;
    }

    private GateDischargeCalculator.SluiceConfig loadSluiceConfig(String siteId) {
        try {
            String sql = "SELECT full_open_free_coeff, width, bottom_elevation, " +
                    "submerged_flow_coeff, controlled_free_coeff, orifice_submerged_coeff, height " +
                    "FROM " + SLUICE_TABLE + " WHERE site = ? ORDER BY version DESC, created_at DESC LIMIT 1";
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, siteId);
            if (rows == null || rows.isEmpty()) {
                log.warn("站点未配置流量计算参数(sluice_discharge): site={}", siteId);
                return null;
            }
            Map<String, Object> row = rows.get(0);

            // 字段可能为字符串或数值，统一按文本解析
            Double m     = parseDoubleOrNull(row.get("full_open_free_coeff"));
            Double phi   = parseDoubleOrNull(row.get("submerged_flow_coeff"));
            Double mu    = parseDoubleOrNull(row.get("controlled_free_coeff"));
            Double mu2   = parseDoubleOrNull(row.get("orifice_submerged_coeff"));
            Double width = parseDoubleOrNull(row.get("width"));
            Double bottom = parseDoubleOrNull(row.get("bottom_elevation"));
            Double height = parseDoubleOrNull(row.get("height"));

            if (m == null || phi == null || mu == null || mu2 == null
                    || width == null || bottom == null || height == null) {
                log.warn("流量计算配置字段缺失或非数值, 跳过: site={}, row={}", siteId, row);
                return null;
            }
            // 守卫6：系数/孔宽/闸底高程 ≤0 视为脏配置
            if (m <= 0 || phi <= 0 || mu <= 0 || mu2 <= 0
                    || width <= 0 || bottom <= 0 || height <= 0) {
                log.warn("流量计算配置异常(存在≤0值), 跳过: site={}, m={}, phi={}, mu={}, mu2={}, width={}, bottom={}, height={}",
                        siteId, m, phi, mu, mu2, width, bottom, height);
                return null;
            }
            return new GateDischargeCalculator.SluiceConfig(siteId, m, phi, mu, mu2, width, bottom, height);
        } catch (Exception e) {
            log.warn("查询流量计算配置失败: site={}, 错误: {}", siteId, e.getMessage());
            return null;
        }
    }

    /** 配置缓存条目（config 为 null 表示查无记录/脏配置，同样短期缓存避免反复查库） */
    private static class SluiceConfigEntry {
        final GateDischargeCalculator.SluiceConfig config;
        final long loadTime;

        SluiceConfigEntry(GateDischargeCalculator.SluiceConfig config, long loadTime) {
            this.config = config;
            this.loadTime = loadTime;
        }
    }

    /** Object → Double（兼容 Number 与文本），解析失败返回 null */
    private Double toDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return parseDoubleOrNull(value);
    }

    private Double parseDoubleOrNull(Object value) {
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private int parseInt(Object value) {
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return Integer.MAX_VALUE;
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

            // 不存在则创建（MQTT仅有闸门数据，type固定为#4#）
            try {
                String deviceId = IdGenerator.generate();
                Timestamp now = new Timestamp(System.currentTimeMillis());
                String sql = "INSERT INTO " + DEVICE_TABLE +
                        " (id, corp_code, created_at, created_by, updated_at, updated_by, name, site, type) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                jdbcTemplate.update(sql, deviceId, corpCode, now, "SYSTEM", now, "SYSTEM", name, siteId, "#4#");
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

        // 水位（站级，每个闸孔行都填充）；守卫：>0且≤1000；
        // 通讯异常哨兵值(FFFFFFFF/4294967295)不丢弃，归一化为-999入库表示设备异常
        if (upZ != null) {
            Double val = parseDoubleSafe(upZ, "up_z");
            if (val == null && isCommErrorText(upZ)) val = -999.0;
            if (val != null && (val == -999 || (val > 0 && val <= MAX_WATER_LEVEL && val != SENSOR_COMM_ERR))) {
                map.put("up_z", val);
            }
        }
        if (downZ != null) {
            Double val = parseDoubleSafe(downZ, "down_z");
            if (val == null && isCommErrorText(downZ)) val = -999.0;
            if (val != null && (val == -999 || (val > 0 && val <= MAX_WATER_LEVEL && val != SENSOR_COMM_ERR))) {
                map.put("down_z", val);
            }
        }

        // 闸门开度 R_{gateNo-1} → open_degree；守卫：物理范围 [0, 50]m，越界不入库；
        // 通讯异常哨兵值(FFFFFFFF)或PLC上报-999：以-999入库表示设备异常(不丢弃)
        String rKey = "R_" + String.format("%03d", gateNo - 1);
        if (fields.containsKey(rKey)) {
            Double val = parseDoubleSafe(fields.get(rKey), "open_degree");
            if (val == null && isCommErrorText(fields.get(rKey))) val = -999.0;
            if (val != null && (val == -999 || (val >= 0 && val <= MAX_OPEN_DEGREE))) {
                map.put("open_degree", val);
            }
        }

        // DI 状态 B_{base}~B_{base+7} → local/remote/...；守卫：仅接受0/1，其他值视为PLC异常不入库
        int bBase = (gateNo - 1) * 16;
        for (int i = 0; i < 8; i++) {
            String bKey = "B_" + String.format("%03d", bBase + i);
            if (fields.containsKey(bKey)) {
                Integer val = parseIntSafe(fields.get(bKey), DI_FIELDS[i]);
                if (val != null && (val == 0 || val == 1)) map.put(DI_FIELDS[i], val);
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

    /** 文本形式的通讯异常哨兵值判断（十六进制FFFFFFFF/十进制4294967295），归一化为-999入库 */
    private static boolean isCommErrorText(String value) {
        if (value == null) return false;
        String t = value.trim();
        return "FFFFFFFF".equalsIgnoreCase(t)
                || "FFFFFFFFFFFFFFFF".equalsIgnoreCase(t)
                || "4294967295".equals(t);
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
