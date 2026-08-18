package com.qgyun.hltgq.hltgqmq.service;

import com.qgyun.hltgq.hltgqmq.util.IdGenerator;
import com.qgyun.hltgq.hltgqmq.config.WaterLevelDatumProperties;

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
import java.util.concurrent.atomic.AtomicLong;

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

    /** 水位基准高程配置（stcd → 高程m，来自 application.properties，可热改配置重启生效） */
    @Autowired
    private WaterLevelDatumProperties waterLevelDatumProperties;

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
        TAG_TABLE_MAP.put("soilData",   SCHEMA + "t_auto_hltgq_water_nmisp_info");
    }

    /** soilData 墒情报文字段名 → nmisp_info 表列名 映射（M10→mten等） */
    private static final Map<String, String> SOIL_FIELD_MAP = new LinkedHashMap<>();
    static {
        SOIL_FIELD_MAP.put("m10",   "mten");
        SOIL_FIELD_MAP.put("m20",   "mtwenty");
        SOIL_FIELD_MAP.put("m30",   "mthirty");
        SOIL_FIELD_MAP.put("m40",   "mforty");
        SOIL_FIELD_MAP.put("m50",   "mfifty");
        SOIL_FIELD_MAP.put("m60",   "msixty");
        SOIL_FIELD_MAP.put("m80",   "meighty");
        SOIL_FIELD_MAP.put("m100",  "mhundred");
    }

    /** gate 表全限定名（riverInfo 闸站水位路由目标） */
    private static final String GATE_TABLE = SCHEMA + "t_auto_hltgq_water_gate";

    /** ===== 数值守卫边界（物理合理性，审计定版） ===== */
    /** 水位上限(m)，河道/闸站水位基准差异大，取宽松值 */
    private static final double MAX_WATER_LEVEL = 1000;
    /** 闸门开度上限(m)，弧形/平面闸门物理开度远小于此值 */
    private static final double MAX_OPEN_DEGREE = 50;
    /** RTU电压上限(V)，直流供电一般 ≤48V */
    private static final double MAX_VOLTAGE = 100;
    /** 瞬时流量上限(m³/s)，中大型闸站单孔过流远小于此值 */
    private static final double MAX_FLOW = 100;
    /** 日降雨量上限(mm)，极端台风日也不超过此值 */
    private static final double MAX_DAILY_RAINFALL = 5000;
    /** 土壤墒情含水量上限(%)，含水量百分比物理上限100 */
    private static final double MAX_SOIL_MOISTURE = 100;
    /** 1h/3h/6h时段降雨量物理上限(mm)，拦截DYP跳变导致的离奇降雨 */
    private static final double MAX_RAINFALL_1H = 500;
    private static final double MAX_RAINFALL_3H = 1500;
    private static final double MAX_RAINFALL_6H = 3000;
    /** 1h水位涨幅绝对上限(m)，拦截水位跳变导致的离奇涨幅 */
    private static final double MAX_HOURLY_RISE = 100;
    /** FFFFFFFF(4294967295) 传感器通讯异常哨兵值 */
    private static final double SENSOR_COMM_ERR = 4294967295.0;
    /** 通讯异常哨兵值归一化入库值：FFFFFFFF → -9991，与设备上报的 -999(设备不存在)区分 */
    private static final double COMM_ERROR_INSERT_VALUE = -9991;
    /** TM时间戳下界：早于2000年视为解析错误(1970/秒级未转换等)，用服务器时间兜底 */
    private static final long TM_MIN_EPOCH_MS = Timestamp.valueOf("2000-01-01 00:00:00").getTime();
    /** TM时间戳超前容忍度：晚于服务器时间2h视为设备时钟错误 */
    private static final long TM_MAX_AHEAD_MS = 2 * 3600000L;
    /** TM时间戳滞后容忍度：早于服务器时间2h视为设备时钟错误(停摆/复位)，用服务器时间兜底 */
    private static final long TM_MAX_BEHIND_MS = 2 * 3600000L;

    /** 滞后分档阈值：滞后2h~24h按时钟停摆兜底服务器时间；超24h按历史补传保留原TM */
    private static final long TM_BACKFILL_THRESHOLD_MS = 24 * 3600000L;

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

    /** 入库失败丢弃的消息计数（便于监控） */
    private final AtomicLong droppedMessageCount = new AtomicLong(0);

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
                // soilData 墒情报文字段名与表列名不一致，先做映射（M10→mten等）
                if ("soilData".equals(tag)) {
                    String mapped = SOIL_FIELD_MAP.get(lowerKey);
                    if (mapped != null) {
                        lowerKey = mapped;
                    }
                }
                // stcd 已添加，避免重复
                if ("stcd".equals(lowerKey)) {
                    continue;
                }
                // wtInfo 的 TF(设备累计流量)由设备计量、误差大不可信，不再入库，
                // tf/timetf/ytf/ttf 四级累计均由服务端基于 Q 梯形积分计算(见 computeWtAccumulations)
                if ("wtInfo".equals(tag) && "tf".equals(lowerKey)) {
                    continue;
                }
                // tm 字段统一用 extractTm() 解析，兼容 ISO 8601 格式和数字时间戳
                if ("tm".equals(lowerKey)) {
                    if (isValidColumn(validColumns, "tm")) {
                        fieldMap.put("tm", extractTm(entity, now));
                    }
                    continue;
                }
                if (!isValidColumn(validColumns, lowerKey)) {
                    continue;
                }
                JsonNode valueNode = entity.get(fieldName);
                fieldMap.put(lowerKey, convertValue(valueNode));
            }

            // === soilData 墒情字段守卫 ===
            // 含水量百分比物理范围 [0,100]，-999(设备不存在/无传感器)与
            // -9991(通讯异常FFFFFFFF归一化)照常入库；
            // 越界或无效值仅剔除该字段，不影响整条报文入库
            if ("soilData".equals(tag)) {
                for (String col : SOIL_FIELD_MAP.values()) {
                    if (!fieldMap.containsKey(col)) continue;
                    Double v = toDbDouble(fieldMap.get(col));
                    if (v == null || (v < 0 && v != -999 && v != COMM_ERROR_INSERT_VALUE) || v > MAX_SOIL_MOISTURE) {
                        log.warn("soilData 墒情值异常, 剔除字段: stcd={}, col={}, value={}",
                                 stcd, col, fieldMap.get(col));
                        fieldMap.remove(col);
                    }
                }
            }

            // === riverInfo 路由 ===
            if ("riverInfo".equals(tag)) {
                if (hasValue(entity, "Z")) {
                    // Z有值 → 通用水位站；先做数值守卫（排除0/负值），
                    // 通讯异常哨兵值(FFFFFFFF)不拦截，以-9991入库表示设备异常
                    if (isCommErrorValue(entity, "Z")) {
                        log.warn("riverInfo 水位设备异常(FFFFFFFF), 以-9991入库: stcd={}", stcd);
                    } else if (!isPositiveNumber(entity, "Z")) {
                        log.warn("riverInfo 水位Z无效, 跳过入库: stcd={}, Z={}", stcd,
                                 hasValue(entity, "Z") ? entity.get("Z").asText() : "null");
                        return;
                    }
                    // 副水位Z1/Z2守卫：异常值仅剔除该字段（不入库），不影响Z主数据；
                    // 通讯异常哨兵值(FFFFFFFF)不剔除，以-9991入库表示设备异常
                    if (hasValue(entity, "Z1")) {
                        if (isCommErrorValue(entity, "Z1")) {
                            log.warn("riverInfo 副水位Z1设备异常(FFFFFFFF), 以-9991入库: stcd={}", stcd);
                        } else if (!isPositiveNumber(entity, "Z1")) {
                            log.warn("riverInfo 副水位Z1异常, 剔除该字段: stcd={}, Z1={}", stcd, entity.get("Z1").asText());
                            fieldMap.remove("z1");
                        }
                    }
                    if (hasValue(entity, "Z2")) {
                        if (isCommErrorValue(entity, "Z2")) {
                            log.warn("riverInfo 副水位Z2设备异常(FFFFFFFF), 以-9991入库: stcd={}", stcd);
                        } else if (!isPositiveNumber(entity, "Z2")) {
                            log.warn("riverInfo 副水位Z2异常, 剔除该字段: stcd={}, Z2={}", stcd, entity.get("Z2").asText());
                            fieldMap.remove("z2");
                        }
                    }
                    // 基准高程修正：入库水位 = 报文水位 + 站点基准高程(水深→海拔)，先守卫后修正
                    applyWaterLevelDatum(fieldMap, stcd);
                    // 继续走下方 river_info 表入库
                } else {
                    // Z无值 → 闸站水位（闸前/闸后）：写 gate 表。
                    // 优先补全 10 分钟窗口内无水位的开度行；无开度行可补时
                    // 生成 gate_no=1 水位行，等 10 分钟开度补全，等不到即归档（开度留空）
                    insertGateWaterLevelFromRiverInfo(entity, site, stcd);
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
            // === volInfo 条件入库：VOL为空/≤0/超过100V均视为无效或异常 ===
            // 通讯异常哨兵值(FFFFFFFF)不拦截，以-9991入库表示设备异常
            if ("volInfo".equals(tag)) {
                if (!hasValue(entity, "VOL")) {
                    log.debug("volInfo 无有效电压数据, 跳过入库, stcd={}", stcd);
                    return;
                }
                if (isCommErrorValue(entity, "VOL")) {
                    log.warn("volInfo 电压设备异常(FFFFFFFF), 以-9991入库: stcd={}", stcd);
                } else {
                    double vol = parseDoubleSafe(entity.get("VOL"));
                    if (Double.isNaN(vol) || vol <= 0 || vol > MAX_VOLTAGE) {
                        log.warn("volInfo 电压异常, 不入库: stcd={}, VOL={}", stcd, entity.get("VOL").asText());
                        return;
                    }
                }
            }
            // === wtInfo 条件入库 ===
            // Q为0属正常(无流量，如冬季枯水期)；Q<0或>100视为异常；
            // 通讯异常哨兵值(FFFFFFFF/4294967295)不拦截，以-9991入库表示设备异常。
            // 报文累计流量TF不再入库(设备计量误差大不可信)，四级累计由服务端计算。
            if ("wtInfo".equals(tag)) {
                JsonNode qNode = entity.get("Q");
                if (qNode == null || qNode.isNull()) {
                    log.debug("wtInfo 缺少流量字段Q, 跳过入库, stcd={}", stcd);
                    return;
                }
                if (isCommErrorValue(entity, "Q")) {
                    log.warn("wtInfo 瞬时流量设备异常(FFFFFFFF), 以-9991入库: stcd={}", stcd);
                } else {
                    double q = parseDoubleSafe(qNode);
                    if (Double.isNaN(q) || q < 0 || q > MAX_FLOW) {
                        log.warn("wtInfo 瞬时流量异常(Q={}), 不入库: stcd={}", q, stcd);
                        return;
                    }
                }
            }
            // === rainInfo 条件入库：DYP≤0说明设备无雨量监测能力或报文异常，跳过入库 ===
            // DRP可为0（今日无雨），DYP是RTU安装以来累计值，为0则设备不匹配；
            // 通讯异常哨兵值(FFFFFFFF)不拦截，以-9991入库表示设备异常
            if ("rainInfo".equals(tag)) {
                if (isCommErrorValue(entity, "DYP")) {
                    log.warn("rainInfo 雨量设备异常(FFFFFFFF), 以-9991入库: stcd={}", stcd);
                } else {
                    double dyp = hasValue(entity, "DYP") ? parseDoubleSafe(entity.get("DYP")) : Double.NaN;
                    if (Double.isNaN(dyp) || dyp <= 0) {
                        log.warn("rainInfo 无有效雨量数据(DYP缺失/≤0), 跳过入库, stcd={}", stcd);
                        return;
                    }
                }
                // DRP日雨量守卫：异常值仅剔除该字段（不入库），不影响整条报文其他字段；
                // 哨兵值(FFFFFFFF)不剔除，以-9991入库表示设备异常
                if (hasValue(entity, "DRP")) {
                    if (isCommErrorValue(entity, "DRP")) {
                        log.warn("rainInfo 日雨量设备异常(FFFFFFFF), DRP以-9991入库: stcd={}", stcd);
                    } else {
                        double drp = parseDoubleSafe(entity.get("DRP"));
                        if (Double.isNaN(drp) || drp < 0 || drp > MAX_DAILY_RAINFALL) {
                            log.warn("rainInfo 日雨量DRP异常, 剔除该字段: stcd={}, DRP={}", stcd, entity.get("DRP").asText());
                            fieldMap.remove("drp");
                        }
                    }
                }
            }
            // === nmIspInfo / pcpInfo 暂不录入，等待后续设备接入 ===
            if ("nmIspInfo".equals(tag) || "pcpInfo".equals(tag)) {
                log.debug("{} 暂不录入, 等待设备接入, stcd={}", tag, stcd);
                return;
            }

            // === 计算字段：水位涨幅、累计降雨、流量四级累计 ===
            if ("riverInfo".equals(tag)) {
                computeWaterLevelRise1h(fieldMap, stcd, validColumns);
            } else if ("rainInfo".equals(tag)) {
                computeRainfall(fieldMap, entity, stcd, validColumns);
            } else if ("wtInfo".equals(tag)) {
                computeWtAccumulations(fieldMap, stcd, validColumns);
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
                    log.warn("去重检查失败, 放行入库: {}", e.getMessage());
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
            long dropped = droppedMessageCount.incrementAndGet();
            log.error("数据入库失败(累计丢弃{}条): {}", dropped, message, e);
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
     * 闸站(#4#): 生成闸孔1设备 "{siteName}1#"，所有站级数据指向它
     * 非闸站已知类型: 生成RTU设备 "{siteName}{类型后缀}#"
     * 非闸站未知类型: 先查是否已有 "{siteName}1#" 闸孔设备，有则复用(epjutj未及时更新场景)；无则兜底 "{siteName}待接入#"
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
                String suffix = SITE_TYPE_DEVICE_MAP.get(typeCode); // 已知类型直接取，未知返回null
                if (suffix != null) {
                    // 已知非闸站类型（水位计/雨量计等），直接使用，不查闸孔设备
                    deviceName = siteName + suffix + "#";
                    deviceType = epjutj;
                } else {
                    // 类型未知：先检查是否已有闸孔1#设备（epjutj可能尚未更新为#4#的闸站）
                    String gateDeviceName = siteName + "1#";
                    String gateDeviceId = findExistingDevice(gateDeviceName);
                    if (gateDeviceId != null) {
                        log.info("站点类型未知但已存在闸孔设备，复用: stcd={}, device={}", stcd, gateDeviceName);
                        deviceCache.putIfAbsent(gateDeviceName, gateDeviceId);
                        return gateDeviceId;
                    }
                    // 无闸孔设备：兜底"待接入"
                    deviceName = siteName + "待接入#";
                    deviceType = null;
                }
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

    /** 仅查询设备是否存在（不创建），返回 device_id 或 null */
    private String findExistingDevice(String deviceName) {
        // 先查缓存
        String cached = deviceCache.get(deviceName);
        if (cached != null) return cached;
        // 缓存未命中则查DB
        try {
            String sql = "SELECT id FROM " + DEVICE_TABLE + " WHERE name = ?";
            List<String> results = jdbcTemplate.queryForList(sql, String.class, deviceName);
            if (results != null && !results.isEmpty()) {
                String id = results.get(0);
                deviceCache.putIfAbsent(deviceName, id);
                return id;
            }
        } catch (Exception e) {
            log.debug("查找设备失败, name={}, 错误: {}", deviceName, e.getMessage());
        }
        return null;
    }

    /**
     * 清理孤儿"待接入"设备。
     * 先遍历所有子表检查是否有数据：有数据则迁移到正确设备，无数据直接删除。
     * 迁移/删除后从 deviceCache 中移除。
     */
    private void cleanupOrphanDevice(String orphanName, String siteId, String correctDeviceId) {
        String orphanId = findExistingDevice(orphanName);
        if (orphanId == null) return; // 没有孤儿设备，无需处理
        if (orphanId.equals(correctDeviceId)) return; // 就是正确设备本身，不删

        // 检查所有入库子表是否已有该设备的记录
        boolean hasData = false;
        for (String tagKey : TAG_TABLE_MAP.keySet()) {
            if ("gateInfo".equals(tagKey)) continue; // 与 gatesInfo 同表，跳过
            String tableName = TAG_TABLE_MAP.get(tagKey);
            try {
                String checkSql = "SELECT COUNT(*) FROM " + tableName + " WHERE device = ?";
                int count = jdbcTemplate.queryForObject(checkSql, Integer.class, orphanId);
                if (count > 0) {
                    hasData = true;
                    break;
                }
            } catch (Exception e) {
                log.debug("检查孤儿设备子表数据失败, table={}, device={}: {}", tableName, orphanId, e.getMessage());
            }
        }

        if (hasData) {
            // 将所有子表的 device 迁移到正确设备
            int migrated = 0;
            for (String tagKey : TAG_TABLE_MAP.keySet()) {
                if ("gateInfo".equals(tagKey)) continue;
                String tableName = TAG_TABLE_MAP.get(tagKey);
                try {
                    String updateSql = "UPDATE " + tableName + " SET device = ? WHERE device = ?";
                    int n = jdbcTemplate.update(updateSql, correctDeviceId, orphanId);
                    if (n > 0) {
                        migrated += n;
                        log.info("device迁移: {} 表 {} 行, orphan={} -> correct={}", tableName, n, orphanId, correctDeviceId);
                    }
                } catch (Exception e) {
                    log.warn("device迁移失败: table={}, orphan={}: {}", tableName, orphanId, e.getMessage());
                }
            }
            log.info("孤儿设备数据迁移完成: orphan={}, correct={}, 共{}行受到影响", orphanName, correctDeviceId, migrated);
        }

        // 删除孤儿设备（此时子表已无引用或本来就没有）
        try {
            String deleteSql = "DELETE FROM " + DEVICE_TABLE + " WHERE id = ? AND name = ? AND site = ?";
            int deleted = jdbcTemplate.update(deleteSql, orphanId, orphanName, siteId);
            if (deleted > 0) {
                deviceCache.remove(orphanName);
                log.info("已删除孤儿设备: name={}, id={}, site={}", orphanName, orphanId, siteId);
            }
        } catch (Exception e) {
            log.warn("删除孤儿设备失败: name={}, 错误: {}", orphanName, e.getMessage());
        }
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
     * 查站点水位基准高程(stcd → 高程m)：配置来自 application.properties
     * (water-level-datum.datum.*)，未配置返回 0（不加高程）。
     * 键大小写不敏感（先精确匹配，再转大写匹配）。
     */
    private double getWaterLevelDatum(String stcd) {
        if (stcd == null || stcd.isEmpty() || waterLevelDatumProperties == null) {
            return 0;
        }
        Map<String, Double> datum = waterLevelDatumProperties.getDatum();
        if (datum == null || datum.isEmpty()) {
            return 0;
        }
        Double v = datum.get(stcd);
        if (v == null) {
            v = datum.get(stcd.toUpperCase());
        }
        return (v != null && v > 0) ? v : 0;
    }

    /**
     * 通用水位站基准高程修正：fieldMap 中已通过守卫的 z/z1/z2 统一加基准高程(水深→海拔)。
     * 仅对有效正值加高程，-9991(通讯异常)/-999(设备不存在)保持原值不加。
     */
    private void applyWaterLevelDatum(Map<String, Object> fieldMap, String stcd) {
        double datum = getWaterLevelDatum(stcd);
        if (datum <= 0) {
            return;
        }
        boolean applied = false;
        if (fieldMap.containsKey("z")) {
            Double z = toDbDouble(fieldMap.get("z"));
            if (z != null && z > 0) {
                fieldMap.put("z", z + datum);
                applied = true;
            }
        }
        if (fieldMap.containsKey("z1")) {
            Double z1 = toDbDouble(fieldMap.get("z1"));
            if (z1 != null && z1 > 0) {
                fieldMap.put("z1", z1 + datum);
            }
        }
        if (fieldMap.containsKey("z2")) {
            Double z2 = toDbDouble(fieldMap.get("z2"));
            if (z2 != null && z2 > 0) {
                fieldMap.put("z2", z2 + datum);
            }
        }
        if (applied) {
            log.info("水位基准高程修正: stcd={}, 高程={}m, 入库水位=报文水位+高程", stcd, datum);
        }
    }

    /**
     * 字段存在且数值为合理水位（>0 且 ≤1000，排除通讯异常哨兵值 4294967295/FFFFFFFF）；
     * 解析失败（非数值文本）视为无效，避免 asDouble 抛异常拖垮整条报文。
     */
    private boolean isPositiveNumber(JsonNode entity, String field) {
        if (!hasValue(entity, field)) return false;
        double v;
        try {
            v = entity.get(field).asDouble();
        } catch (Exception e) {
            return false;
        }
        return v > 0 && v <= MAX_WATER_LEVEL && v != SENSOR_COMM_ERR;
    }

    /**
     * 闸站水位（Z1/Z2）→ gate 表（闸站数据归闸站表，不写 river_info）：
     * 1. 优先 UPDATE 10 分钟补全窗口内无水位（up_z IS NULL）、且采集时间(tm)与
     *    本水位报文最接近的一批开度行，行转完整（多站点、多批次报文互不污染）；
     * 2. 窗口内无可补的开度行 → INSERT gate_no=1 水位行（open_degree 留空），
     *    作为"待补全"行等 10 分钟开度（gatesInfo 到达时补全），等不到即归档。
     * 已入库行仅允许在 10 分钟补全窗口内补全，窗口外冻结归档不再改动。
     */
    private void insertGateWaterLevelFromRiverInfo(JsonNode entity, String siteId, String stcd) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Timestamp tm = extractTm(entity, now);

        // 有效水位(>0)照常入库；通讯异常哨兵值(FFFFFFFF)以-9991入库表示设备异常；其余视为无效(-1)
        double z1 = isCommErrorValue(entity, "Z1") ? COMM_ERROR_INSERT_VALUE
                : (isPositiveNumber(entity, "Z1") ? entity.get("Z1").asDouble() : -1);
        double z2 = isCommErrorValue(entity, "Z2") ? COMM_ERROR_INSERT_VALUE
                : (isPositiveNumber(entity, "Z2") ? entity.get("Z2").asDouble() : -1);
        boolean z1Valid = z1 > 0 || z1 == COMM_ERROR_INSERT_VALUE;
        boolean z2Valid = z2 > 0 || z2 == COMM_ERROR_INSERT_VALUE;
        if (!z1Valid && !z2Valid) {
            log.debug("riverInfo 无有效闸站水位, 跳过入库, stcd={}", stcd);
            return;
        }
        // 基准高程修正：入库水位 = 报文水位 + 站点基准高程(水深→海拔)，先守卫后修正。
        // 仅对有效正值加高程，-9991(通讯异常)保持原值不加
        double datum = getWaterLevelDatum(stcd);
        if (datum > 0) {
            if (z1 > 0) z1 += datum;
            if (z2 > 0) z2 += datum;
            log.info("闸站水位基准高程修正: stcd={}, 高程={}m", stcd, datum);
        }

        // Z1/Z2有正值确定是闸站，直接用首闸孔设备，不依赖 epjutj
        String siteName = getSiteName(siteId);
        String device = lookupOrCreateDeviceByName(siteName + "1#", siteId, "#4#");
        if (device == null) {
            log.error("riverInfo→gate 设备缺失, 跳过: stcd={}", stcd);
            return;
        }
        // 确保 stcdDeviceCache 指向正确的首闸孔设备（覆盖可能存在的"待接入"缓存）
        String oldCached = stcdDeviceCache.put(stcd, device);
        if (oldCached != null && !oldCached.equals(device)) {
            log.info("riverInfo已纠正stcd设备缓存: stcd={}, 旧设备={}, 新设备={}", stcd, oldCached, device);
        }
        // 清理可能已创建的孤儿"待接入"设备（msgInfo 先于 riverInfo 到达时产生）
        cleanupOrphanDevice(siteName + "待接入#", siteId, device);

        // 1) 优先补全 10 分钟窗口内无水位的开度行（水位补齐，行转完整）。
        // 时间对齐：只补采集时间(tm)与水位报文最接近的一批（同一tm的多孔行一起补），
        // 避免把本小时水位误贴到窗口内其他采集时间的开度行上（多站点/多批次报文场景）。
        Timestamp windowStart = new Timestamp(now.getTime() - 600000L);
        Timestamp windowEnd = new Timestamp(now.getTime() + 600000L);
        List<Timestamp> pendingTms = jdbcTemplate.queryForList(
                "SELECT tm FROM " + GATE_TABLE +
                " WHERE stcd = ? AND open_degree IS NOT NULL AND up_z IS NULL AND tm >= ? AND tm <= ?",
                Timestamp.class, stcd, windowStart, windowEnd);
        Timestamp targetTm = nearestTm(pendingTms, tm);
        if (targetTm != null) {
            StringBuilder updateSql = new StringBuilder("UPDATE " + GATE_TABLE +
                    " SET updated_at = ?, updated_by = 'SYSTEM'");
            List<Object> updateParams = new ArrayList<>();
            updateParams.add(now);
            if (z1Valid) {
                updateSql.append(", up_z = ?");
                updateParams.add(z1);
            }
            if (z2Valid) {
                updateSql.append(", down_z = ?");
                updateParams.add(z2);
            }
            updateSql.append(" WHERE stcd = ? AND open_degree IS NOT NULL AND up_z IS NULL AND tm = ?");
            updateParams.add(stcd);
            updateParams.add(targetTm);
            int completed = jdbcTemplate.update(updateSql.toString(), updateParams.toArray());
            if (completed > 0) {
                log.info("riverInfo水位已补全 {} 条采集时间最近的开度行: stcd={}, tm={}, Z1={}, Z2={}",
                         completed, stcd, targetTm, z1Valid ? z1 : "null", z2Valid ? z2 : "null");
            }
        }

        // 2) 生成 gate_no=1 水位行（待补全缓存行，10 分钟内等开度补全，等不到即归档）
        // 去重：同 stcd+tm+gate_no=1 已存在则跳过（防止RabbitMQ重投）
        try {
            String checkSql = "SELECT COUNT(*) FROM " + GATE_TABLE + " WHERE stcd = ? AND tm = ? AND gate_no = '1'";
            int count = jdbcTemplate.queryForObject(checkSql, Integer.class, stcd, tm);
            if (count > 0) {
                log.debug("gate表水位行已存在, 跳过: stcd={}, tm={}", stcd, tm);
                return;
            }
        } catch (Exception e) {
            log.debug("gate表去重检查失败, 放行: {}", e.getMessage());
        }

        Set<String> gateCols = tableColumnsCache.getOrDefault("t_auto_hltgq_water_gate", Collections.emptySet());
        Map<String, Object> fm = new LinkedHashMap<>();
        fm.put("id",          IdGenerator.generate());
        fm.put("corp_code",   corpCode);
        fm.put("created_at",  now);
        fm.put("created_by",  "SYSTEM");
        fm.put("updated_at",  now);
        fm.put("updated_by",  "SYSTEM");
        if (gateCols.isEmpty() || gateCols.contains("site"))        fm.put("site",         siteId);
        if (gateCols.isEmpty() || gateCols.contains("device"))      fm.put("device",       device);
        if (gateCols.isEmpty() || gateCols.contains("stcd"))        fm.put("stcd",         stcd);
        if (gateCols.isEmpty() || gateCols.contains("gate_no"))     fm.put("gate_no",      "1");
        if (gateCols.isEmpty() || gateCols.contains("tm"))          fm.put("tm",           tm);
        if (gateCols.isEmpty() || gateCols.contains("date"))        fm.put("date",         tm);
        if (gateCols.isEmpty() || gateCols.contains("ctime"))       fm.put("ctime",        tm);
        if (gateCols.isEmpty() || gateCols.contains("status"))      fm.put("status",       "#1#");
        if (z1Valid && (gateCols.isEmpty() || gateCols.contains("up_z")))   fm.put("up_z",   z1);
        if (z2Valid && (gateCols.isEmpty() || gateCols.contains("down_z"))) fm.put("down_z", z2);
        // open_degree 留空：10 分钟窗口内等 gatesInfo 补全，等不到即归档（无开度）

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
        log.info("riverInfo闸站水位行入库: stcd={}, gate_no=1, Z1={}, Z2={}", stcd,
                 z1Valid ? z1 : "null", z2Valid ? z2 : "null");
    }

    /**
     * gatesInfo 闸门开度路由：将 Gates1/2/3 → gate 表多行（闸孔级开度，gate_no=1/2/3）
     * 每闸孔独立设备，命名: {站点名}{闸孔号}#
     * 1号孔优先补全 10 分钟窗口内、采集时间(tm)与本报文最接近的一条待补全水位缓存行；
     * 各闸孔行按"读最近水位 + INSERT 一次成型"写入。
     * 行入库后仅允许在 10 分钟补全窗口内被 riverInfo 补水位，窗口外冻结归档不再改动。
     */
    private void insertGateFromGatesInfo(JsonNode entity, String siteId, String stcd) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Set<String> gateCols = tableColumnsCache.getOrDefault("t_auto_hltgq_water_gate", Collections.emptySet());
        String siteName = getSiteName(siteId);

        // 使用报文 TM，兜底服务器时间
        Timestamp tm = extractTm(entity, now);

        // 取该站最新闸站水位（读 gate 表近 2h 记录，仅读不改），与开度一并写入本批次闸孔行
        double[] waterLevels = getLatestWaterLevels(stcd);

        int inserted = 0;
        for (int i = 1; i <= 10; i++) {
            String fieldName = "Gates" + i;
            if (!hasValue(entity, fieldName)) continue; // 跳过null字段，继续处理后续闸孔

            double openDegree = parseDoubleSafe(entity.get(fieldName));
            // 通讯异常哨兵值(文本FFFFFFFF/数值4294967295)不再跳过该闸孔，统一以
            // open_degree=-9991 入库表示设备通讯异常（展示层按 -9991 识别）；
            // 设备上报的 -999(设备不存在)保持原值入库，与 -9991 区分；
            // 其他非数值文本或越界值仍跳过，避免展示层出现离奇开度
            if (isCommErrorValue(entity, fieldName)) {
                openDegree = COMM_ERROR_INSERT_VALUE;
            } else if (openDegree == -999) {
                // 设备上报 -999(设备不存在)，保持原值入库
            } else if (Double.isNaN(openDegree)) {
                log.warn("gatesInfo 开度非数值, 跳过闸孔{}: stcd={}, value={}", i, stcd, entity.get(fieldName).asText());
                continue;
            } else if (openDegree < 0 || openDegree > MAX_OPEN_DEGREE) {
                log.warn("gatesInfo 开度异常, 跳过闸孔{}: stcd={}, open_degree={}", i, stcd, openDegree);
                continue;
            }

            // 1号孔优先补全 10 分钟窗口内待补全的水位缓存行（riverInfo 先到生成的 gate_no=1 行）。
            // 时间对齐：只补采集时间(tm)与开度报文最接近的一条，
            // 避免同一条开度被复制到窗口内多个采集时间的水位行上。
            if (i == 1) {
                try {
                    Timestamp windowStart = new Timestamp(now.getTime() - 600000L);
                    Timestamp windowEnd = new Timestamp(now.getTime() + 600000L);
                    List<Map<String, Object>> pendingRows = jdbcTemplate.queryForList(
                            "SELECT id, tm FROM " + GATE_TABLE +
                            " WHERE stcd = ? AND gate_no = '1' AND open_degree IS NULL AND up_z IS NOT NULL AND tm >= ? AND tm <= ?",
                            stcd, windowStart, windowEnd);
                    String targetId = null;
                    long bestDiff = Long.MAX_VALUE;
                    for (Map<String, Object> row : pendingRows) {
                        Timestamp rowTm = toDbTimestamp(row.get("tm"));
                        if (rowTm == null || row.get("id") == null) continue;
                        long diff = Math.abs(rowTm.getTime() - tm.getTime());
                        if (diff < bestDiff) {
                            bestDiff = diff;
                            targetId = String.valueOf(row.get("id"));
                        }
                    }
                    if (targetId != null) {
                        String completeSql = "UPDATE " + GATE_TABLE +
                                " SET open_degree = ?, updated_at = ?, updated_by = 'SYSTEM' WHERE id = ?";
                        int completed = jdbcTemplate.update(completeSql, openDegree, now, targetId);
                        if (completed > 0) {
                            log.info("gatesInfo已补全采集时间最近的水位缓存行: stcd={}, id={}", stcd, targetId);
                        }
                    } else {
                        // 窗口外兜底：开度迟到超10分钟时，按同采集时间(tm)对齐补全水位缓存行，
                        // 防止 1# 孔开度被去重键(stcd+tm+gate_no)挡住而永久丢失
                        List<Map<String, Object>> sameTmRows = jdbcTemplate.queryForList(
                                "SELECT id FROM " + GATE_TABLE +
                                " WHERE stcd = ? AND gate_no = '1' AND open_degree IS NULL AND up_z IS NOT NULL AND tm = ?",
                                stcd, tm);
                        if (!sameTmRows.isEmpty() && sameTmRows.get(0).get("id") != null) {
                            String sameTmId = String.valueOf(sameTmRows.get(0).get("id"));
                            String completeSql = "UPDATE " + GATE_TABLE +
                                    " SET open_degree = ?, updated_at = ?, updated_by = 'SYSTEM' WHERE id = ?";
                            int completed = jdbcTemplate.update(completeSql, openDegree, now, sameTmId);
                            if (completed > 0) {
                                log.warn("gatesInfo窗口外补全同tm水位缓存行(开度迟到): stcd={}, id={}, tm={}",
                                         stcd, sameTmId, tm);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.debug("gatesInfo补全水位行失败, 放行: {}", e.getMessage());
                }
            }

            String deviceName = siteName + i + "#";
            String deviceId = lookupOrCreateDeviceByName(deviceName, siteId, "#4#");
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
            if (waterLevels[0] >= 0 && (gateCols.isEmpty() || gateCols.contains("up_z")))   fm.put("up_z",   waterLevels[0]);
            if (waterLevels[1] >= 0 && (gateCols.isEmpty() || gateCols.contains("down_z"))) fm.put("down_z", waterLevels[1]);

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

        // 刷新 stcdDeviceCache 指向正确的首闸孔设备（覆盖 epjutj=NULL 时可能创建的"待接入"设备）
        if (inserted > 0) {
            String gate1DeviceName = siteName + "1#";
            String gate1DeviceId = findExistingDevice(gate1DeviceName);
            if (gate1DeviceId != null) {
                String old = stcdDeviceCache.put(stcd, gate1DeviceId);
                if (old != null && !old.equals(gate1DeviceId)) {
                    log.info("已纠正stcd设备缓存: stcd={}, 旧设备={}, 新设备={}", stcd, old, gate1DeviceId);
                }
                // 清理无数据的孤儿"待接入"设备
                cleanupOrphanDevice(siteName + "待接入#", siteId, gate1DeviceId);
            }
        }

        log.info("gatesInfo闸门开度入库成功: stcd={}, 闸孔数={}, table={}", stcd, inserted, GATE_TABLE);
    }

    /**
     * 计算1h水位涨幅（cm）：当前Z - 1小时前Z，写入 water_level_rise1h 字段
     */
    private void computeWaterLevelRise1h(Map<String, Object> fieldMap, String stcd, Set<String> validColumns) {
        if (!isValidColumn(validColumns, "water_level_rise1h")) return;

        Object zObj = fieldMap.get("z");
        // convertValue 对文本数值返回 String，必须用 toDbDouble 统一解析，否则涨幅永远不计算
        Double zVal = toDbDouble(zObj);
        // 守卫：当前水位无效(缺失/≤0/通讯异常-9991/设备不存在-999)时不计算涨幅，z本身照常入库
        if (zVal == null || zVal <= 0) return;

        String device = fieldMap.containsKey("device") ? (String) fieldMap.get("device") : null;
        // 查询基准用本行测量时间tm：历史补传行的"1h涨幅"应相对其自身时间轴，
        // 若用系统当前时间会把跨月水位差误标为1h涨幅
        Timestamp baseTm = toDbTimestamp(fieldMap.get("tm"));
        if (baseTm == null) return;
        Timestamp oneHourAgo = new Timestamp(baseTm.getTime() - 3600000);
        Timestamp twoHoursAgo = new Timestamp(baseTm.getTime() - 7200000);
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
                double prevZ = results.get(0);
                // 守卫：历史水位异常(存量42亿/越界)时不计算涨幅，避免写入离奇涨幅
                if (!isValidWaterLevelValue(prevZ)) {
                    log.warn("1h涨幅计算跳过, 历史水位异常: stcd={}, prevZ={}", stcd, prevZ);
                    return;
                }
                double rise = zVal - prevZ;
                // 守卫：涨幅超物理上限视为水位跳变异常，不写入（z本身照常入库）
                if (Math.abs(rise) > MAX_HOURLY_RISE) {
                    log.warn("1h水位涨幅超物理上限, 不写入: stcd={}, rise={}", stcd, rise);
                    return;
                }
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
        // 守卫：DYP为通讯异常哨兵值(入库-9991)时不计算时段降雨，避免离奇差值
        if (isCommErrorValue(entity, "DYP")) return;
        double currentDyp = entity.get("DYP").asDouble();

        String device = fieldMap.containsKey("device") ? (String) fieldMap.get("device") : null;

        // 查询基准用本行测量时间tm（与1h涨幅一致，历史补传行按自身时间轴计算时段降雨）
        Timestamp baseTm = toDbTimestamp(fieldMap.get("tm"));
        if (baseTm == null) return;

        if (isValidColumn(validColumns, "rainfall1h")) {
            Double prev = queryPreviousDyp(stcd, device, 3600000, baseTm);
            if (prev != null) {
                double diff = trunc2(currentDyp - prev);
                if (diff >= 0) {
                    // 守卫：时段降雨超物理上限视为DYP跳变异常，不写入（dyp本身照常入库）
                    if (diff > MAX_RAINFALL_1H) {
                        log.warn("rainfall1h超物理上限, 不写入: stcd={}, diff={}", stcd, diff);
                    } else {
                        fieldMap.put("rainfall1h", diff);
                    }
                } else {
                    log.debug("rainfall1h计算异常(DYP回退), stcd={}, currentDyp={}, prevDyp={}, diff={}", stcd, currentDyp, prev, diff);
                }
            }
        }
        if (isValidColumn(validColumns, "rainfall3h")) {
            Double prev = queryPreviousDyp(stcd, device, 3 * 3600000, baseTm);
            if (prev != null) {
                double diff = trunc2(currentDyp - prev);
                if (diff >= 0) {
                    if (diff > MAX_RAINFALL_3H) {
                        log.warn("rainfall3h超物理上限, 不写入: stcd={}, diff={}", stcd, diff);
                    } else {
                        fieldMap.put("rainfall3h", diff);
                    }
                } else {
                    log.debug("rainfall3h计算异常(DYP回退), stcd={}, currentDyp={}, prevDyp={}, diff={}", stcd, currentDyp, prev, diff);
                }
            }
        }
        if (isValidColumn(validColumns, "rainfall6h")) {
            Double prev = queryPreviousDyp(stcd, device, 6 * 3600000, baseTm);
            if (prev != null) {
                double diff = trunc2(currentDyp - prev);
                if (diff >= 0) {
                    if (diff > MAX_RAINFALL_6H) {
                        log.warn("rainfall6h超物理上限, 不写入: stcd={}, diff={}", stcd, diff);
                    } else {
                        fieldMap.put("rainfall6h", diff);
                    }
                } else {
                    log.debug("rainfall6h计算异常(DYP回退), stcd={}, currentDyp={}, prevDyp={}, diff={}", stcd, currentDyp, prev, diff);
                }
            }
        }
    }

    /**
     * 计算站点四级累计流量(服务端梯形积分)：timetf时段 / tf日 / ytf年 / ttf总，单位 m³。
     * 查 wt_nfo 该站最近一行(RabbitMQ行，stcd匹配)，时段增量 delta = (lastQ + q) / 2 × Δt(Δt = 本行tm - 上行tm)。
     * 日累计：同天继承 lastTf + delta，跨天/首行从 delta 起算；
     * 年累计：同年继承 lastYtf + delta，跨年/首行从 delta 起算；
     * 总累计：永远继承 lastTtf + delta。
     * 改造前的旧行 tf 是设备累计(不可信)且无 ytf，以 lastYtf==null 判定旧行 → tf 不继承；
     * 间隔超2h视为数据断层(断报/停机)，delta=0 不积分防止累计虚增；
     * 1~1.5h属正常区间抖动静默积分，1.5~2h缺批容忍并积分(近似值)但WARN提醒排查；乱序补传(Δt<0)同样不积分；
     * Q为通讯异常哨兵值(-9991)时不积分(累计列继承上值保持平顶)。
     */
    private void computeWtAccumulations(Map<String, Object> fieldMap, String stcd, Set<String> validColumns) {
        Double q = toDbDouble(fieldMap.get("q"));
        Timestamp tm = toDbTimestamp(fieldMap.get("tm"));
        if (q == null || tm == null) {
            return;
        }

        double timetf = 0, tf = 0, ytf = 0, ttf = 0;
        try {
            Timestamp dayStart = Timestamp.valueOf(tm.toLocalDateTime().toLocalDate().atStartOfDay());
            Timestamp yearStart = Timestamp.valueOf(tm.toLocalDateTime().toLocalDate().withDayOfYear(1).atStartOfDay());

            Timestamp lastTm = null;
            Double lastQ = null;
            Double lastTf = null;
            Double lastYtf = null;
            Double lastTtf = null;
            String sql = "SELECT tm, q, tf, ytf, ttf FROM " + SCHEMA + "t_auto_hltgq_water_wt_nfo" +
                    " WHERE stcd = ? AND tm < ? ORDER BY tm DESC LIMIT 1";
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, stcd, tm);
            if (rows != null && !rows.isEmpty()) {
                Map<String, Object> row = rows.get(0);
                lastTm = toDbTimestamp(row.get("tm"));
                lastQ = toDbDouble(row.get("q"));
                lastTf = toDbDouble(row.get("tf"));
                lastYtf = toDbDouble(row.get("ytf"));
                lastTtf = toDbDouble(row.get("ttf"));
            }

            double delta = 0;
            if (q >= 0 && lastTm != null && lastQ != null && lastQ >= 0) {
                long dtMs = tm.getTime() - lastTm.getTime();
                double dtSec = dtMs / 1000.0;
                if (dtMs < 0) {
                    log.warn("wtInfo流量积分时间倒挂(补传乱序), 该段不积分: stcd={}, 间隔={}s", stcd, dtSec);
                } else if (dtSec < 1800) {
                    // 1h周期报文不可能出现<30min的相邻行，短间隔必为TM兜底错标行/乱序补传所致，
                    // 积分会把错标行与正常行之间的假时段算入累计，虚增流量
                    log.warn("wtInfo流量积分间隔过小(疑似TM兜底错标/乱序), 该段不积分: stcd={}, 间隔={}s", stcd, dtSec);
                } else if (dtSec > 7200) {
                    log.warn("wtInfo流量积分间隔超2h(断报/停机), 该段不积分: stcd={}, 间隔={}s", stcd, dtSec);
                } else {
                    // 报文1h周期，延迟≤30min(5400s)属正常区间抖动，超过才视为疑似缺报
                    if (dtSec > 5400) {
                        log.warn("wtInfo流量积分间隔超1.5h(疑似缺报), 积分为近似值: stcd={}, 间隔={}s", stcd, dtSec);
                    }
                    delta = (lastQ + q) / 2.0 * dtSec;
                }
            }
            // 首行/上行无效/Q异常：delta=0，各级累计从0起算

            timetf = delta;
            boolean sameDay = lastTm != null && lastTm.getTime() >= dayStart.getTime();
            boolean sameYear = lastTm != null && lastTm.getTime() >= yearStart.getTime();
            // tf继承要求 lastYtf!=null：改造前旧行 tf 是设备累计(不可信)，只有改造后新行之间才继承日累计
            tf = (sameDay && lastTf != null && lastYtf != null) ? lastTf + delta : delta;
            ytf = (sameYear && lastYtf != null) ? lastYtf + delta : delta;
            ttf = (lastTtf != null) ? lastTtf + delta : delta;
        } catch (Exception e) {
            log.warn("wtInfo计算累计流量失败, 累计列不入库: stcd={}, {}", stcd, e.getMessage());
            return;
        }
        if (isValidColumn(validColumns, "timetf")) fieldMap.put("timetf", timetf);
        if (isValidColumn(validColumns, "tf")) fieldMap.put("tf", tf);
        if (isValidColumn(validColumns, "ytf")) fieldMap.put("ytf", ytf);
        if (isValidColumn(validColumns, "ttf")) fieldMap.put("ttf", ttf);
    }

    /** 查询基准时间前 intervalMs 窗口内的DYP累计值（DYP永不重置，差值始终≥0）；基准=本行测量时间tm */
    private Double queryPreviousDyp(String stcd, String device, long intervalMs, Timestamp baseTm) {
        long lowerBoundMs = intervalMs * 2;
        Timestamp from = new Timestamp(baseTm.getTime() - lowerBoundMs);
        Timestamp to   = new Timestamp(baseTm.getTime() - intervalMs);
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
                double prev = results.get(0);
                // 守卫：历史DYP异常(存量0/负值/42亿哨兵值)时不参与差值计算，避免离奇降雨量
                if (prev > 0 && prev != SENSOR_COMM_ERR) {
                    return prev;
                }
                log.warn("历史DYP异常, 不参与降雨量计算: stcd={}, prevDyp={}", stcd, prev);
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
     * 安全解析数值节点：非数值文本(如FFFFFFFF)返回NaN，表示传感器通讯异常
     */
    private double parseDoubleSafe(JsonNode node) {
        try {
            return node.isTextual() ? Double.parseDouble(node.asText()) : node.asDouble();
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }

    /**
     * 将JsonNode值转为Java对象。
     * 通讯异常哨兵值统一归一化：文本"FFFFFFFF"/数值4294967295 → -9991(设备通讯异常)，
     * 全类型报文通用——异常值也入库(-9991)，展示层按 -9991 识别设备异常；
     * 设备上报的 -999(设备不存在)保持原值入库，与 -9991 区分。
     */
    private Object convertValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            String text = node.asText();
            if (isCommErrorText(text)) {
                return "-9991";
            }
            return text;
        }
        if (node.isInt()) {
            return node.asInt();
        }
        if (node.isLong()) {
            long v = node.asLong();
            if (v == (long) SENSOR_COMM_ERR) {
                return COMM_ERROR_INSERT_VALUE;
            }
            return v;
        }
        if (node.isDouble() || node.isFloat()) {
            double d = node.asDouble();
            if (d == SENSOR_COMM_ERR) {
                return COMM_ERROR_INSERT_VALUE;
            }
            return d;
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        return node.asText();
    }

    /**
     * 判断字段值是否为设备通讯异常哨兵值（文本"FFFFFFFF"/数值4294967295）。
     * 哨兵值不再拦截丢弃，统一以 -9991 入库表示设备通讯异常，全类型报文/设备通用；
     * 与设备上报的 -999(设备不存在)区分。
     */
    private boolean isCommErrorValue(JsonNode entity, String field) {
        if (!hasValue(entity, field)) return false;
        JsonNode node = entity.get(field);
        if (node.isTextual()) {
            return isCommErrorText(node.asText());
        }
        return node.isNumber() && node.asDouble() == SENSOR_COMM_ERR;
    }

    /** 文本形式的通讯异常哨兵值判断（十六进制FFFFFFFF或十进制4294967295） */
    private static boolean isCommErrorText(String text) {
        if (text == null) return false;
        String t = text.trim();
        return "FFFFFFFF".equalsIgnoreCase(t)
                || "FFFFFFFFFFFFFFFF".equalsIgnoreCase(t)
                || "4294967295".equals(t);
    }

    // ======================== Gate 辅助方法 ========================

    /** 从 entity 中提取测量时间 TM，解析失败或时间异常则用兜底时间 */
    private Timestamp extractTm(JsonNode entity, Timestamp fallback) {
        if (!hasValue(entity, "TM")) return fallback;
        try {
            Timestamp ts;
            JsonNode tmNode = entity.get("TM");
            if (tmNode.isNumber()) {
                long epochMs = tmNode.asLong();
                // 防御：秒级时间戳(10位)按毫秒解析会变成1970年，统一转毫秒；0/负值视为无效
                if (epochMs <= 0) return fallback;
                if (epochMs < 100_000_000_000L) {
                    epochMs *= 1000L;
                }
                ts = new Timestamp(epochMs);
            } else {
                // ISO 8601 "yyyy-MM-ddTHH:mm:ss" → 替换 T 为空格，兼容 Timestamp.valueOf()
                ts = Timestamp.valueOf(tmNode.asText().replace('T', ' '));
            }
            // 时间合理性守卫：
            // 早于2000年(1970解析错误)、超前服务器时间2h(设备时钟快) → 服务器时间兜底；
            // 滞后分档：2h~24h视为设备时钟停摆/复位(如TM停在00:00:00)，兜底服务器时间，
            // 避免同批报文tm相同触发stcd+tm去重碰撞丢数据(9000000006站事故)；
            // 滞后超24h视为RTU断网恢复后的合法补传(连续历史时间序列)，保留原TM入库，
            // 若兜底成当前时间会把历史数据错标时间并污染当天流量积分。
            if (ts.getTime() < TM_MIN_EPOCH_MS
                    || ts.getTime() > fallback.getTime() + TM_MAX_AHEAD_MS) {
                log.warn("TM时间戳异常(早于2000年/超前2h), 使用服务器时间: TM={}",
                         entity.get("TM").asText());
                return fallback;
            }
            if (ts.getTime() < fallback.getTime() - TM_MAX_BEHIND_MS) {
                if (fallback.getTime() - ts.getTime() <= TM_BACKFILL_THRESHOLD_MS) {
                    log.warn("TM时钟停摆疑似(滞后2h~24h), 使用服务器时间: TM={}",
                             entity.get("TM").asText());
                    return fallback;
                }
                // 历史补传：保留原测量时间入库，时间轴正确，历史行不参与实时累计积分
                log.debug("历史补传数据(滞后超24h), 保留原TM入库: TM={}", entity.get("TM").asText());
            }
            return ts;
        } catch (Exception e) {
            log.debug("无法解析TM字段, 使用兜底时间: {}", entity.get("TM"));
            return fallback;
        }
    }

    /**
     * 取 stcd 最新闸站水位（仅读 gate 表，不改库）：
     * 查近 2 小时内最近一条带 up_z 的闸孔行（含水位缓存行和完整行）。
     * 只读历史数据，不产生任何等待、缓存或滞留。返回 {upZ, downZ}，缺失为 -1。
     */
    private double[] getLatestWaterLevels(String stcd) {
        try {
            Timestamp window = new Timestamp(System.currentTimeMillis() - 7200000L);
            String selectSql = "SELECT up_z, down_z FROM " + GATE_TABLE +
                    " WHERE stcd = ? AND up_z IS NOT NULL AND tm >= ? ORDER BY tm DESC LIMIT 1";
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(selectSql, stcd, window);
            if (!rows.isEmpty()) {
                Map<String, Object> row = rows.get(0);
                Double up = toDbDouble(row.get("up_z"));
                Double down = toDbDouble(row.get("down_z"));
                // 守卫：存量异常水位(42亿哨兵值/越界)不得传播到新开度行，无效视为缺失
                double upZ = (up != null && isValidWaterLevelValue(up)) ? up : -1;
                double downZ = (down != null && isValidWaterLevelValue(down)) ? down : -1;
                return new double[]{upZ, downZ};
            }
        } catch (Exception e) {
            log.warn("查询gate表最近闸站水位失败, stcd={}: {}", stcd, e.getMessage());
        }
        return new double[]{-1, -1};
    }

    /**
     * 水位类数值合理性校验（>0 且 ≤MAX_WATER_LEVEL，排除通讯异常哨兵值）；
     * 供历史水位读回（涨幅计算、开度行继承水位）与存量异常值防传播使用。
     */
    private static boolean isValidWaterLevelValue(double v) {
        return v > 0 && v <= MAX_WATER_LEVEL && v != SENSOR_COMM_ERR;
    }

    /** Object → Double（兼容 Number 与文本），转换失败返回 null */
    private Double toDbDouble(Object value) {
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).doubleValue();
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** 从候选采集时间集合中选与目标时间差最小者；无候选返回 null */
    private Timestamp nearestTm(List<Timestamp> candidates, Timestamp target) {
        Timestamp best = null;
        long bestDiff = Long.MAX_VALUE;
        for (Timestamp c : candidates) {
            if (c == null) continue;
            long diff = Math.abs(c.getTime() - target.getTime());
            if (diff < bestDiff) {
                bestDiff = diff;
                best = c;
            }
        }
        return best;
    }

    /** Object → Timestamp（兼容 Timestamp/Date/文本），转换失败返回 null */
    private Timestamp toDbTimestamp(Object value) {
        if (value == null) return null;
        if (value instanceof Timestamp) return (Timestamp) value;
        if (value instanceof java.util.Date) return new Timestamp(((java.util.Date) value).getTime());
        try {
            return Timestamp.valueOf(String.valueOf(value));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 存量清理：每小时删除超过 1 小时的 gate_no=0 水位占位行。
     * 当前 riverInfo 闸站水位(Z1/Z2)直接写 gate 表 gate_no=1 水位行，
     * gatesInfo 开度经 10 分钟补全窗口与水位同行合并，占位行已从根上杜绝产生，
     * 本任务仅用于清理改造前遗留的历史占位行。
     */
    @Scheduled(fixedRate = 3600000)
    public void cleanupStaleGatePlaceholders() {
        try {
            Timestamp staleCutoff = new Timestamp(System.currentTimeMillis() - 3600000L);
            String deleteSql = "DELETE FROM " + GATE_TABLE +
                    " WHERE gate_no = '0' AND tm < ?";
            int deleted = jdbcTemplate.update(deleteSql, staleCutoff);
            if (deleted > 0) {
                log.info("兜底清理超时占位行: {} 条", deleted);
            }
        } catch (Exception e) {
            log.warn("兜底清理占位行失败: {}", e.getMessage());
        }
    }

    /**
     * 开度断报告警：每小时检查近 2 小时内有闸站水位入库(gate_no=1 水位行)
     * 但无任何开度入库的站点，WARN 提醒排查开度报文上报链路。
     * 阈值 ≥2 条水位行，排除单条偶然错位导致的误报。
     */
    @Scheduled(fixedRate = 3600000)
    public void checkGateDegreeGapAlarm() {
        try {
            Timestamp windowStart = new Timestamp(System.currentTimeMillis() - 7200000L);
            String sql = "SELECT w.stcd, COUNT(*) AS cnt FROM " + GATE_TABLE + " w" +
                    " WHERE w.stcd IS NOT NULL AND w.gate_no = '1'" +
                    " AND w.open_degree IS NULL AND w.up_z IS NOT NULL AND w.tm >= ?" +
                    " AND NOT EXISTS (" +
                    "  SELECT 1 FROM " + GATE_TABLE + " o" +
                    "  WHERE o.stcd = w.stcd AND o.open_degree IS NOT NULL AND o.tm >= ?" +
                    " )" +
                    " GROUP BY w.stcd HAVING COUNT(*) >= 2";
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, windowStart, windowStart);
            for (Map<String, Object> row : rows) {
                log.warn("闸站开度报文疑似断报: stcd={}, 近2h水位行{}条且无任何开度入库, 请排查开度上报链路",
                         row.get("stcd"), row.get("cnt"));
            }
        } catch (Exception e) {
            log.warn("开度断报告警检查失败: {}", e.getMessage());
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
            // 无条件更新：跨天后即使已经 #1# 也要刷新 updated_at，
            // 否则 checkOfflineSites 会在 24h 后误标为离线
            String sql = "UPDATE " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef " +
                         "SET zebpsu = '#1#', updated_at = ?, updated_by = 'SYSTEM' " +
                         "WHERE id = ?";
            int rows = jdbcTemplate.update(sql, now, siteId);
            if (rows > 0) {
                log.debug("站点标记在线: site={}", siteId);
            }
        } catch (Exception e) {
            log.debug("标记站点在线失败, site={}: {}", siteId, e.getMessage());
        }
    }

    /**
     * 每小时检查：直接查所有入库表，判断站点 24h 内是否有数据到达。
     * 不依赖 updated_at 代理字段，以实际入库记录为准。
     * <p>
     * 数据源覆盖：
     * <pre>
     * RabbitMQ (stcd 匹配 iofhpi):
     *   msg_info   — 通信日志（msgInfo，MSG 为空时跳过）
     *   vol_info   — RTU 电压（volInfo，VOL≤0 时跳过）
     *   wt_nfo     — 流量（wtInfo，Q缺失/通讯异常/异常值 时跳过）
     *   river_info — 水位（riverInfo，Z 空时跳过；闸站 Z1/Z2 走 gate 表）
     *   rain_info  — 雨量（rainInfo，DYP≤0 时跳过）
     *   gate       — 闸门开度/水位（gateInfo/gatesInfo 开度，riverInfo 闸站水位 Z1/Z2）
     *   nmisp_info — 土壤墒情（soilData）
     *
     * MQTT (site 直接匹配 id):
     *   gate       — 闸门监测（无 stcd，仅 site 字段）
     *
     * 不入库的表（已跳过，无需检查）：
     *   pcp_info   — pcpInfo 暂不录入
     * </pre>
     */
    @Scheduled(fixedRate = 3600000)
    public void checkOfflineSites() {
        try {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            Timestamp cutoff = new Timestamp(now.getTime() - 86400000L); // 24h前
            // 对所有入库表做 UNION ALL：任一表有 24h 内数据 → 在线，全部无数据 → 离线
            String sql = "UPDATE " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef s " +
                         "SET zebpsu = '#2#', updated_at = ?, updated_by = 'SYSTEM' " +
                         "WHERE zebpsu IS DISTINCT FROM '#2#' " +
                         "AND NOT EXISTS (" +
                         "  SELECT 1 FROM " + SCHEMA + "t_auto_hltgq_water_msg_info   WHERE stcd = s.iofhpi AND tm >= ?" +
                         "  UNION ALL " +
                         "  SELECT 1 FROM " + SCHEMA + "t_auto_hltgq_water_vol_info   WHERE stcd = s.iofhpi AND tm >= ?" +
                         "  UNION ALL " +
                         "  SELECT 1 FROM " + SCHEMA + "t_auto_hltgq_water_wt_nfo     WHERE stcd = s.iofhpi AND tm >= ?" +
                         "  UNION ALL " +
                         "  SELECT 1 FROM " + SCHEMA + "t_auto_hltgq_water_river_info WHERE stcd = s.iofhpi AND tm >= ?" +
                         "  UNION ALL " +
                         "  SELECT 1 FROM " + SCHEMA + "t_auto_hltgq_water_rain_info  WHERE stcd = s.iofhpi AND tm >= ?" +
                         "  UNION ALL " +
                         "  SELECT 1 FROM " + SCHEMA + "t_auto_hltgq_water_gate       WHERE stcd = s.iofhpi AND tm >= ?" +
                         "  UNION ALL " +
                         "  SELECT 1 FROM " + SCHEMA + "t_auto_hltgq_water_gate       WHERE site = s.id     AND tm >= ?" +
                         "  UNION ALL " +
                         "  SELECT 1 FROM " + SCHEMA + "t_auto_hltgq_water_nmisp_info WHERE stcd = s.iofhpi AND tm >= ?" +
                         ")";
            int rows = jdbcTemplate.update(sql, now,
                    cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff, cutoff);
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
