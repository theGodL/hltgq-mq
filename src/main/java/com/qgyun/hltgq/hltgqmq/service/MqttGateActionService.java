package com.qgyun.hltgq.hltgqmq.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qgyun.hltgq.hltgqmq.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MQTT 闸门动作记录（DZJL topic）解析入库服务
 *
 * 设备方新增 DZJL 主题用于监控闸门开启/关闭动作（动作时间 + 执行状态），
 * 与 HLTData 的 PLC 状态快照链路（MqttGateDataService，gate 表）互补：
 * 快照记录"当前是什么状态"，DZJL 记录"什么时候发生了开/关动作"。
 *
 * 解析口径（按设备方数据点组态表 HLT20260915 定稿）：
 * - 报文为三平行数组 names/vals/updatetime（单帧含全部 28 个动作点，非动作点
 *   updatetime 为空串）；标签名形如 {前缀}_ZM{孔号}_{KQ|GB}.PV；
 * - 只取 val=1 的点落库：KQ=开启动作、GB=关闭动作，动作时间取该点 updatetime；
 *   站点按前缀查 PREFIX_MAP，闸孔号取 ZM 后数字，并自动匹配同孔设备
 *   （设备名口径：站名+孔号+“#”，如 南山寺节制闸1#）；
 * - 原始报文全量留痕 msg_info（msg='DZJL'）并冗余写入每行动作 raw；
 * - 相同动作（站+孔+标签+动作时间）由入库去重拦截 Broker 重投/持续帧。
 */
@Service
public class MqttGateActionService {

    private static final Logger log = LoggerFactory.getLogger(MqttGateActionService.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /** 告警服务：动作报文到达视同站点通信恢复，联动关闭失联告警 */
    @Autowired
    private AlertService alertService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String SCHEMA = "\"qixiao-apaas\".";

    @Value("${app.corp-code:hltgq}")
    private String corpCode;

    private static final String ACTION_TABLE = SCHEMA + "t_auto_hltgq_water_gate_action";
    private static final String MSG_TABLE    = SCHEMA + "t_auto_hltgq_water_msg_info";

    /** 动作表有效列名缓存（未建表时为空集合，降级为仅留痕） */
    private Set<String> actionColumns = Collections.emptySet();

    /** msg_info 有效列名缓存 */
    private Set<String> msgColumns = Collections.emptySet();

    /** 已打过结构诊断日志的键集合签名（每种结构只打一次 info，防日志刷屏） */
    private final Set<String> diagDumped = ConcurrentHashMap.newKeySet();

    /** site 名称 → siteId 缓存 */
    private final ConcurrentHashMap<String, String> siteCache = new ConcurrentHashMap<>();

    /** 闸孔设备 id 缓存（key=siteId|设备名） */
    private final ConcurrentHashMap<String, String> deviceCache = new ConcurrentHashMap<>();

    /**
     * 已入库动作指纹缓存（站|孔|标签|动作时间）：闸门开启期间设备方每秒推一帧全量，
     * 绝大多数是已入库动作的重复帧；内存命中的帧零 DB、零 INFO 日志，
     * DB 去重仍保留兜底跨重启/缓存清空后的补投。
     */
    private final Set<String> insertedFingerprints = ConcurrentHashMap.newKeySet();

    /** 指纹缓存上限：超出清空重建（DB 去重兜底，最况仅退化一次查库） */
    private static final int FP_CACHE_LIMIT = 5000;

    @PostConstruct
    public void init() {
        try {
            List<String> cols = jdbcTemplate.queryForList(
                    "SELECT column_name FROM information_schema.columns " +
                    "WHERE table_schema = 'qixiao-apaas' AND table_name = 't_auto_hltgq_water_gate_action'",
                    String.class);
            actionColumns = new HashSet<>();
            for (String col : cols) {
                actionColumns.add(col.toLowerCase());
            }
            if (actionColumns.isEmpty()) {
                log.warn("闸门动作记录表不存在, DZJL 报文将仅留痕不落动作表; 请先执行 10_create_gate_action.sql");
            } else {
                log.info("已加载动作记录表列名元数据: {} 列", actionColumns.size());
            }
        } catch (Exception e) {
            log.error("加载动作记录表列名元数据失败", e);
        }
        try {
            List<String> cols = jdbcTemplate.queryForList(
                    "SELECT column_name FROM information_schema.columns " +
                    "WHERE table_schema = 'qixiao-apaas' AND table_name = 't_auto_hltgq_water_msg_info'",
                    String.class);
            msgColumns = new HashSet<>();
            for (String col : cols) {
                msgColumns.add(col.toLowerCase());
            }
        } catch (Exception e) {
            log.error("加载报文流水表列名元数据失败", e);
        }
    }

    /**
     * 处理一条 DZJL 报文：帧判定 → 逐条落动作表 → 留痕。
     * 闸门开启期间设备方每秒推一帧（28 点全量、大多为历史动作的重复帧），
     * 因此先做内存级动作指纹比对：无新动作的帧直接返回，不产生任何 DB 写入与 INFO 日志，
     * 保证单线程 MQTT 回调不被高频帧堵塞。
     */
    public void process(String topic, String payload) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Map<String, Object> firstRow = null;
        try {
            JsonNode root = objectMapper.readTree(payload);
            dumpDiagnostic(root, payload);

            List<Map<String, Object>> rows = new ArrayList<>();
            List<LabelPoint> points = parseLabelFrame(root);
            if (points != null) {
                // 设备方口径：开启/关闭是两个点，取“标签变成1”那条的 updatetime 作为动作时间；
                // 值不为1的点不是动作时刻跳过；已入库动作（内存指纹命中）的重复帧直接忽略
                int actionPoints = 0;
                for (LabelPoint p : points) {
                    if (!isOne(p.val)) continue;
                    actionPoints++;
                    Map<String, Object> row = buildRowFromPoint(p, payload, now);
                    String fp = fingerprint(row);
                    if (fp != null && insertedFingerprints.contains(fp)) continue;
                    rows.add(row);
                }
                if (!rows.isEmpty()) {
                    // 仅新动作帧打 INFO（含全文供追溯）；常态帧降 debug，避免每秒刷屏
                    log.info("DZJL 帧含新动作点: topic={}, 动作点 {} 个, 待入库 {} 个, payload={}",
                            topic, actionPoints, rows.size(), truncate(payload));
                } else if (actionPoints > 0) {
                    log.debug("DZJL 帧动作点均已在库, 跳过: topic={}, 动作点 {} 个", topic, actionPoints);
                } else {
                    log.debug("DZJL 周期帧无动作点, 跳过: topic={}", topic);
                }
            } else {
                List<Map<String, String>> items = new ArrayList<>();
                collectItems(root, items);
                for (Map<String, String> kv : items) {
                    if (!kv.isEmpty()) rows.add(buildRow(kv, payload, now));
                }
            }

            int ok = 0;
            int dup = 0;
            int fail = 0;
            for (Map<String, Object> row : rows) {
                if (firstRow == null) firstRow = row;
                int r = insertAction(row);
                String fp = fingerprint(row);
                if (r == 1) {
                    ok++;
                    if (fp != null) cacheFingerprint(fp);
                } else if (r == 0) {
                    dup++;
                    // 库中已存在（如重启后 Broker 补投），入缓存防后续帧反复查库
                    if (fp != null) cacheFingerprint(fp);
                } else {
                    fail++;
                }
            }
            if (!rows.isEmpty()) {
                log.info("DZJL 动作记录: topic={}, 解析 {} 条, 新入库 {} 条, 重复跳过 {} 条, 失败 {} 条",
                        topic, rows.size(), ok, dup, fail);
            }

            // 报文留痕：仅新动作帧落 msg_info（每秒重复帧全量留痕会快速膨胀；
            // 动作内容已冗余在 action 行 raw 列）；解析失败路径仍全量留痕保留证据
            if (ok > 0) {
                insertMsgLog(topic, payload, firstRow, now);
            }
        } catch (Exception e) {
            log.error("DZJL 报文解析失败, 仅留痕: topic={}, err={}", topic, e.getMessage());
            try {
                insertMsgLog(topic, payload, null, now);
            } catch (Exception ignore) {
                // 留痕失败仅降级日志，不抛出
            }
        }
    }

    /** 动作指纹：与 DB 去重口径一致（site 优先，缺失退回站名）；无动作时间的行不参与指纹 */
    private static String fingerprint(Map<String, Object> row) {
        Object t = row.get("action_time");
        if (t == null) return null;
        String siteKey = row.get("site") != null ? text(row.get("site")) : text(row.get("site_name"));
        return siteKey + "|" + text(row.get("gate_no")) + "|" + text(row.get("action")) + "|" + t;
    }

    /** 指纹入缓存（上限防护：清空重建，DB 去重兜底） */
    private void cacheFingerprint(String fp) {
        if (insertedFingerprints.size() >= FP_CACHE_LIMIT) {
            insertedFingerprints.clear();
        }
        insertedFingerprints.add(fp);
    }

    /** 日志截断预览（报文超过 2000 字时截断） */
    private static String truncate(String payload) {
        return payload.length() > 2000 ? payload.substring(0, 2000) + "..." : payload;
    }

    // ======================== 解析 ========================

    /** 三平行数组标签帧（names/vals/updatetime）中的一个标签点 */
    private static final class LabelPoint {
        final String name; // 标签名，如 NSS_ZM1_KQ.PV
        final String val;  // 值文本，动作点=1
        final String time; // 该点值变化时标，如 2026/10/14 15:18:16.8
        LabelPoint(String name, String val, String time) {
            this.name = name; this.val = val; this.time = time;
        }
    }

    /**
     * 解析设备方“三平行数组”标签帧：{"names":[...],"vals":[...],"updatetime":[...]}
     *（字段名大小写不敏感并兼容常见别名）；三数组长度不等时取最短。
     * 非该形态返回 null，走通用宽容路径（collectItems）。
     */
    private List<LabelPoint> parseLabelFrame(JsonNode root) {
        if (root == null || !root.isObject()) return null;
        JsonNode names = null;
        JsonNode vals = null;
        JsonNode times = null;
        Iterator<Map.Entry<String, JsonNode>> it = root.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            if (!e.getValue().isArray()) continue;
            String n = normalize(e.getKey());
            if ("names".equals(n) || "name".equals(n) || "tagnames".equals(n) || "labelnames".equals(n)) {
                names = e.getValue();
            } else if ("vals".equals(n) || "values".equals(n) || "val".equals(n) || "value".equals(n)) {
                vals = e.getValue();
            } else if ("updatetime".equals(n) || "updatetimes".equals(n) || "times".equals(n)) {
                times = e.getValue();
            }
        }
        if (names == null || vals == null || times == null) return null;
        int size = Math.min(names.size(), Math.min(vals.size(), times.size()));
        if (size <= 0) return null;
        List<LabelPoint> points = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            points.add(new LabelPoint(names.get(i).asText(), vals.get(i).asText(), times.get(i).asText()));
        }
        return points;
    }

    /** 标签值是否代表“动作触发”（1/1.0/true；设备方口径：标签变成1的时刻即动作时间） */
    private static boolean isOne(String v) {
        if (v == null) return false;
        String t = v.trim();
        return "1".equals(t) || "1.0".equals(t) || "true".equalsIgnoreCase(t);
    }

    /** 标签名解析出的闸孔动作信息 */
    private static final class GateAction {
        final String gateNo;     // 闸孔号，如 "1"
        final String actionType; // open=开启(KQ) / close=关闭(GB)
        GateAction(String gateNo, String actionType) {
            this.gateNo = gateNo; this.actionType = actionType;
        }
    }

    /** 标签名正则：{站点前缀}_ZM{孔号}_{KQ|GB}，如 NSS_ZM1_KQ / QSJSZ_ZM2_GB（入参已去 .PV 后缀） */
    private static final java.util.regex.Pattern GATE_LABEL =
            java.util.regex.Pattern.compile("^[A-Za-z]+_ZM(\\d+)_(KQ|GB)$", java.util.regex.Pattern.CASE_INSENSITIVE);

    /** 按组态表口径解析标签名：KQ=开启动作、GB=关闭动作；非闸门动作标签返回 null */
    private static GateAction parseGateLabel(String label) {
        if (label == null) return null;
        java.util.regex.Matcher m = GATE_LABEL.matcher(label.trim());
        if (!m.matches()) return null;
        return new GateAction(m.group(1), "KQ".equalsIgnoreCase(m.group(2)) ? "open" : "close");
    }

    /** null 安全的文本化（去重参数兜底用） */
    private static String text(Object v) {
        return v == null ? "" : String.valueOf(v);
    }

    /**
     * 收集待落库的动作条目（值归一化为字符串 KV）：
     * - 根节点为数组：逐元素（元素必须为对象，取其一层字段）；
     * - 根节点为对象且自身含标量字段：作为单条记录；
     * - 根节点为对象且某字段值是数组（如 {"DZJL":[{...},{...}]}）：数组元素逐条，
     *   外层其余标量字段合并进每条记录（设备方常见的"外层公共字段+内层明细"形态）。
     */
    private void collectItems(JsonNode root, List<Map<String, String>> out) {
        if (root.isArray()) {
            for (JsonNode el : root) {
                if (el.isObject()) out.add(flatten(el));
            }
            return;
        }
        if (!root.isObject()) {
            return;
        }
        Map<String, String> outer = new LinkedHashMap<>();
        List<JsonNode> arrayEls = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> it = root.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            if (e.getValue().isArray()) {
                for (JsonNode el : e.getValue()) {
                    if (el.isObject()) arrayEls.add(el);
                }
            } else if (e.getValue().isValueNode()) {
                outer.put(e.getKey(), e.getValue().asText());
            }
        }
        if (arrayEls.isEmpty()) {
            if (!outer.isEmpty()) out.add(outer);
            return;
        }
        for (JsonNode el : arrayEls) {
            Map<String, String> kv = new LinkedHashMap<>(outer);
            kv.putAll(flatten(el));
            out.add(kv);
        }
    }

    /** 一层对象扁平化：标量值取 asText，嵌套对象/数组取紧凑 JSON 文本 */
    private Map<String, String> flatten(JsonNode node) {
        Map<String, String> kv = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            JsonNode v = e.getValue();
            kv.put(e.getKey(), v.isValueNode() ? v.asText() : v.toString());
        }
        return kv;
    }

    /**
     * 动作行组装：候选键名归一化（小写、去 _/空格）后匹配动作/时间/状态/闸孔/站点，
     * 匹配不到即留空——raw 全量保留保证信息不丢失，字段映射待真实样例校准。
     */
    private Map<String, Object> buildRow(Map<String, String> kv, String rawPayload, Timestamp now) {
        String siteName = pick(kv, "site", "station", "sitename", "siteid", "stcd", "zdmc", "zbmc", "站名", "站点", "站点名称");
        String gateNo   = pick(kv, "gateno", "gate", "gn", "gateid", "zhm", "gateindex", "闸孔", "闸门号", "孔号");
        String action   = pick(kv, "action", "act", "actiontype", "actioncode", "dz", "dzlx", "op", "operation", "kz", "动作", "动作类型", "动作类型名称");
        String state    = pick(kv, "status", "state", "result", "zt", "jg", "actionresult", "执行状态", "状态", "结果");
        String timeText = pick(kv, "time", "tm", "datetime", "timestamp", "actiontime", "acttime", "sj", "dtsj", "ts", "动作时间", "时间");

        // 站点匹配：字段可能是站点名前缀（NSS 等）或完整站名
        String siteId = null;
        String resolvedSiteName = siteName;
        if (siteName != null && !siteName.isEmpty()) {
            String mapped = MqttGateDataService.PREFIX_MAP.get(siteName.trim().toUpperCase());
            if (mapped != null) resolvedSiteName = mapped;
            siteId = lookupSiteByName(resolvedSiteName);
            if (siteId == null) siteId = lookupSiteByName(siteName.trim());
        }

        Timestamp actionTime = parseTime(timeText);

        // 组态表口径补充：标签名含 _ZM{n}_{KQ|GB} 时解析闸孔号/动作类型，并匹配同孔设备
        GateAction ga = action == null ? null : parseGateLabel(action.trim());
        String finalGateNo = gateNo == null ? null : digitsOnly(gateNo);
        if (finalGateNo == null && ga != null) finalGateNo = ga.gateNo;

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id",          IdGenerator.generate());
        map.put("corp_code",   corpCode);
        map.put("site",        siteId);
        map.put("site_name",   resolvedSiteName);
        map.put("gate_no",     finalGateNo);
        map.put("device",      lookupGateDevice(siteId, resolvedSiteName, finalGateNo));
        map.put("action",      action);
        map.put("action_type", ga == null ? null : ga.actionType);
        map.put("action_time", actionTime);
        map.put("state",       state);
        map.put("raw",         rawPayload);
        map.put("tm",          now);
        map.put("created_at",  now);
        map.put("created_by",  "SYSTEM");
        map.put("updated_at",  now);
        map.put("updated_by",  "SYSTEM");
        return map;
    }

    /**
     * 三平行数组标签点 → 动作行：
     * action=标签名全称（去 .PV 后缀，原样保留供追溯），action_time=该点 updatetime，
     * state=上报值；站点按前缀（NSS/QSDZ/QSJSZ/SMH）查 PREFIX_MAP，
     * 闸孔号/动作类型按组态表 {前缀}_ZM{孔号}_{KQ|GB} 解析，并匹配同孔设备。
     */
    private Map<String, Object> buildRowFromPoint(LabelPoint p, String rawPayload, Timestamp now) {
        String label = p.name.endsWith(".PV") ? p.name.substring(0, p.name.length() - 3) : p.name;
        String siteId = null;
        String siteName = null;
        String prefix = label.contains("_") ? label.substring(0, label.indexOf('_')) : label;
        String mapped = MqttGateDataService.PREFIX_MAP.get(prefix.toUpperCase());
        if (mapped != null) {
            siteName = mapped;
            siteId = lookupSiteByName(mapped);
        }
        GateAction ga = parseGateLabel(label);
        String gateNo = ga == null ? null : ga.gateNo;

        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id",          IdGenerator.generate());
        map.put("corp_code",   corpCode);
        map.put("site",        siteId);
        map.put("site_name",   siteName);
        map.put("gate_no",     gateNo);
        map.put("device",      lookupGateDevice(siteId, siteName, gateNo));
        map.put("action",      label);
        map.put("action_type", ga == null ? null : ga.actionType);
        map.put("action_time", parseTime(p.time));
        map.put("state",       p.val);
        map.put("raw",         rawPayload);
        map.put("tm",          now);
        map.put("created_at",  now);
        map.put("created_by",  "SYSTEM");
        map.put("updated_at",  now);
        map.put("updated_by",  "SYSTEM");
        return map;
    }

    /** 在 KV 中按候选键（归一化后）找值，未命中返回 null */
    private static String pick(Map<String, String> kv, String... candidates) {
        for (String c : candidates) {
            for (Map.Entry<String, String> e : kv.entrySet()) {
                if (normalize(e.getKey()).equals(c) && e.getValue() != null && !e.getValue().isEmpty()) {
                    return e.getValue().trim();
                }
            }
        }
        return null;
    }

    /** 键名归一化：小写、去掉下划线/连字符/空格 */
    private static String normalize(String key) {
        return key == null ? "" : key.toLowerCase().replace("_", "").replace("-", "").replace(" ", "");
    }

    /** 提取纯数字（"闸孔1"/"1#"/"01" → "1"），无数字返回原文 */
    private static String digitsOnly(String v) {
        String d = v.replaceAll("[^0-9]", "");
        return d.isEmpty() ? v : d;
    }

    /**
     * 动作时间宽容解析：10 位秒/13 位毫秒时间戳，及常见字符串格式；
     * 小数秒归一到 3 位再解析——JS 时间串 "2026/10/14 15:18:16.8" 的 .8 是 800ms，
     * 不直接给 SimpleDateFormat 按毫秒语义误读；无法识别返回 null（原始文本在 raw 可追溯）。
     */
    private static Timestamp parseTime(String text) {
        if (text == null || text.isEmpty()) return null;
        String t = text.trim();
        try {
            if (t.matches("\\d{13}")) return new Timestamp(Long.parseLong(t));
            if (t.matches("\\d{10}")) return new Timestamp(Long.parseLong(t) * 1000L);
        } catch (NumberFormatException ignore) {
            // 落到字符串格式解析
        }
        // 小数秒归一：.8/.82 → .800/.820（十进制秒补零到毫秒 3 位）
        java.util.regex.Matcher ms = java.util.regex.Pattern
                .compile("^(.*\\d{2}:\\d{2}:\\d{2})\\.(\\d{1,2})$").matcher(t);
        if (ms.matches()) {
            t = ms.group(1) + "." + (ms.group(2) + "00").substring(0, 3);
        }
        String[] patterns = {
                "yyyy-MM-dd HH:mm:ss.SSS", "yyyy/MM/dd HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSS",
                "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", "yyyy/MM/dd HH:mm:ss",
                "yyyy-MM-dd HH:mm"
        };
        for (String p : patterns) {
            try {
                return new Timestamp(new SimpleDateFormat(p).parse(t).getTime());
            } catch (Exception ignore) {
                // 尝试下一个格式
            }
        }
        return null;
    }

    /**
     * 结构诊断：同一种顶层键集合首次出现时打 info（含报文前 2000 字），
     * 部署后从日志核对真实字段名；重复结构不再打印防刷屏。
     */
    private void dumpDiagnostic(JsonNode root, String payload) {
        try {
            Set<String> keys = new TreeSet<>();
            JsonNode sample = root;
            if (root.isArray()) {
                sample = root.size() > 0 ? root.get(0) : root;
            } else if (root.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> it = root.fields();
                boolean arrayFound = false;
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> e = it.next();
                    if (e.getValue().isArray() && e.getValue().size() > 0) {
                        keys.add(e.getKey() + "[]");
                        sample = e.getValue().get(0);
                        arrayFound = true;
                    }
                }
                if (!arrayFound) sample = root;
            }
            if (sample.isObject()) {
                sample.fieldNames().forEachRemaining(keys::add);
            }
            String sig = String.join(",", keys);
            if (!sig.isEmpty() && diagDumped.add(sig)) {
                String preview = payload.length() > 2000 ? payload.substring(0, 2000) + "..." : payload;
                log.info("DZJL 报文结构诊断(首次出现, 供字段映射校准): keys={}, payload={}", sig, preview);
            }
        } catch (Exception e) {
            log.debug("DZJL 结构诊断输出失败: {}", e.getMessage());
        }
    }

    // ======================== 入库 ========================

    /**
     * 落动作表：按列元数据过滤（表未建→提示一次并跳过）；返回 2=表缺失/失败, 1=成功, 0=重复跳过。
     * 去重口径：site 或 site_name + 闸孔 + 动作时间 + 动作 已存在则视为重复
     * （Broker 重投/进程重启补投场景，cleanSession=false + QoS1 会产生重复消息）。
     */
    private int insertAction(Map<String, Object> row) {
        if (actionColumns.isEmpty()) {
            log.debug("闸门动作记录表未就绪, DZJL 行仅留痕: site={}, action={}", row.get("site"), row.get("action"));
            return 2;
        }
        // 有动作时间才做跨重投去重；无时间信息（仅 raw）无法判定重复，直接落。
        // 参数一律给非 null 文本：KingbaseES 对未定类型的 null 绑定参数会报类型推断错误，
        // 曾导致去重检查异常被降级放行、同一动作被持续帧反复入库（COALESCE 两侧归 '' 比较）。
        Timestamp actionTime = (Timestamp) row.get("action_time");
        if (actionTime != null) {
            try {
                String dupSql = "SELECT COUNT(*) FROM " + ACTION_TABLE +
                        " WHERE COALESCE(site, site_name, '') = ?" +
                        "   AND COALESCE(gate_no, '') = ?" +
                        "   AND COALESCE(action, '') = ?" +
                        "   AND action_time = ?";
                Integer count = jdbcTemplate.queryForObject(dupSql, Integer.class,
                        text(row.get("site") != null ? row.get("site") : row.get("site_name")),
                        text(row.get("gate_no")), text(row.get("action")), actionTime);
                if (count != null && count > 0) {
                    return 0;
                }
            } catch (Exception e) {
                log.warn("DZJL 去重检查失败, 放行: {}", e.getMessage());
            }
        }

        StringBuilder cols = new StringBuilder();
        StringBuilder phs = new StringBuilder();
        List<Object> vals = new ArrayList<>();
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (!actionColumns.contains(e.getKey().toLowerCase())) continue;
            if (cols.length() > 0) { cols.append(", "); phs.append(", "); }
            cols.append(e.getKey());
            phs.append("?");
            vals.add(e.getValue());
        }
        try {
            String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", ACTION_TABLE, cols, phs);
            jdbcTemplate.update(sql, vals.toArray());
            log.info("DZJL 动作已入库: site={}, gateNo={}, action={}, actionTime={}, state={}",
                    row.get("site"), row.get("gate_no"), row.get("action"), row.get("action_time"), row.get("state"));
            // 报文到达视同站点通信恢复
            Object siteId = row.get("site");
            if (siteId != null) {
                markSiteOnline(String.valueOf(siteId));
            }
            return 1;
        } catch (Exception e) {
            log.warn("DZJL 动作入库失败: site={}, action={}: {}", row.get("site"), row.get("action"), e.getMessage());
            return 2;
        }
    }

    /** 报文监测流水留痕：msg=DZJL，msg_text=原始报文全文；旁路写入失败不影响主流程 */
    private void insertMsgLog(String topic, String payload, Map<String, Object> firstRow, Timestamp now) {
        if (msgColumns.isEmpty() || !msgColumns.contains("msg_text")) {
            log.debug("msg_info 列元数据缺失, 跳过 DZJL 留痕: topic={}", topic);
            return;
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id",         IdGenerator.generate());
        m.put("corp_code",  corpCode);
        m.put("created_at", now);
        m.put("created_by", "SYSTEM");
        m.put("updated_at", now);
        m.put("updated_by", "SYSTEM");
        if (msgColumns.contains("site") && firstRow != null && firstRow.get("site") != null) {
            m.put("site", firstRow.get("site"));
        }
        if (msgColumns.contains("tm"))       m.put("tm",       now);
        if (msgColumns.contains("sendtm"))   m.put("sendtm",   now);
        if (msgColumns.contains("msg"))      m.put("msg",      topic);
        if (msgColumns.contains("msg_text")) m.put("msg_text", payload);

        StringBuilder cols = new StringBuilder();
        StringBuilder phs = new StringBuilder();
        List<Object> vals = new ArrayList<>();
        for (Map.Entry<String, Object> e : m.entrySet()) {
            if (!msgColumns.contains(e.getKey().toLowerCase())) continue;
            if (cols.length() > 0) { cols.append(", "); phs.append(", "); }
            cols.append(e.getKey());
            phs.append("?");
            vals.add(e.getValue());
        }
        try {
            jdbcTemplate.update(String.format("INSERT INTO %s (%s) VALUES (%s)", MSG_TABLE, cols, phs), vals.toArray());
        } catch (Exception e) {
            log.warn("DZJL 报文留痕失败: topic={}: {}", topic, e.getMessage());
        }
    }

    // ======================== 站点状态 ========================

    /** 按站点名称查 zzkaec → id（带缓存，未匹配站名缓存 null 用 Optional 语义防重复查库） */
    private String lookupSiteByName(String siteName) {
        if (siteName == null || siteName.isEmpty()) return null;
        String cached = siteCache.get(siteName);
        if (cached != null) return cached.isEmpty() ? null : cached;
        try {
            List<String> results = jdbcTemplate.queryForList(
                    "SELECT id FROM " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef WHERE zzkaec = ?",
                    String.class, siteName);
            if (results != null && !results.isEmpty()) {
                siteCache.put(siteName, results.get(0));
                return results.get(0);
            }
        } catch (Exception e) {
            log.warn("查找站点失败, siteName={}, 错误: {}", siteName, e.getMessage());
        }
        siteCache.put(siteName, "");
        return null;
    }

    /**
     * 闸孔设备查找：设备名口径与 PLC 快照链路一致（站名+孔号+“#”，如 南山寺节制闸1#），
     * type 限定 #4#；找到缓存 id，未找到缓存空串（告警一次防刷屏），返回 null 动作行照常落库。
     */
    private String lookupGateDevice(String siteId, String siteName, String gateNo) {
        if (siteId == null || siteName == null || gateNo == null || gateNo.isEmpty()) return null;
        String devName = siteName + gateNo + "#";
        String key = siteId + "|" + devName;
        String cached = deviceCache.get(key);
        if (cached != null) return cached.isEmpty() ? null : cached;
        try {
            List<String> ids = jdbcTemplate.queryForList(
                    "SELECT id FROM " + SCHEMA + "t_auto_hltgq_water_device WHERE site = ? AND name = ? AND type LIKE '%#4#%'",
                    String.class, siteId, devName);
            if (ids != null && !ids.isEmpty()) {
                deviceCache.put(key, ids.get(0));
                return ids.get(0);
            }
        } catch (Exception e) {
            log.warn("查找闸孔设备失败, name={}, 错误: {}", devName, e.getMessage());
        }
        deviceCache.put(key, "");
        log.warn("闸孔设备未匹配, 动作记录 device 留空: site={}, 设备名口径={}", siteId, devName);
        return null;
    }

    /**
     * 动作报文到达即标记站点在线（条件式 UPDATE 幂等去重，跨天刷新，与 MqttGateDataService 对齐）；
     * 状态变化时联动关闭该站失联告警。
     */
    private void markSiteOnline(String siteId) {
        try {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            int rows = jdbcTemplate.update(
                    "UPDATE " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef " +
                    "SET zebpsu = '#1#', updated_at = ?, updated_by = 'SYSTEM' " +
                    "WHERE id = ? AND (zebpsu IS DISTINCT FROM '#1#' OR updated_at < date_trunc('day', now()))",
                    now, siteId);
            if (rows > 0) {
                alertService.closeOfflineAlerts(siteId);
            }
        } catch (Exception e) {
            log.debug("DZJL 标记站点在线失败, site={}: {}", siteId, e.getMessage());
        }
    }
}
