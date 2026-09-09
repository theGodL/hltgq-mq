package com.qgyun.hltgq.hltgqmq.service;

import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 到报率/缺测率统计服务：供统计大屏各模块调用（到报明细/缺测明细/采集状态/服务状态/卡片聚合）。
 * <p>
 * 口径（与产品对齐）：
 * <ul>
 *   <li>报到（通信层）：雨量站按 4 小时窗（RTU 无雨 4h 一报、有雨加密）、RabbitMQ 站按 1 小时窗、
 *       MQTT 闸站按 10 分钟窗，窗内有任何报文(msg_info 流水)即到报。</li>
 *   <li>缺测（数据层）：窗内无有效数据入库即缺测；雨量站按 4 小时窗、RabbitMQ 站按 1 小时窗、
 *       MQTT 闸站按 30 分钟窗（对齐 gate 表 30 分钟批量入库节奏）。</li>
 *   <li>有效数据按站点类型主监测要素判定：水位 z>0、雨量 dyp>0 或有 rainInfo 报文、流量 q>=0、
 *       墒情 mten>=0、闸站 gate 有有效水位/开度；复合类型任一主要素有效即正常；无类型站点不参与缺测。</li>
 *   <li>应报/应测窗数只统计今日 0 点至当前时刻已结束的完整窗。</li>
 *   <li>本月平均到报率：从报文流水上线日（msg_info 本月最早有数据的日期）起逐日平均。</li>
 *   <li>采集状态统计按采集周期（=缺测窗）计窗；成功=有报文且有有效数据的窗（交集），
 *       保证 collected=success+failed 恒成立。</li>
 * </ul>
 */
@Service
public class ArrivalStatsService {

    private static final Logger log = LoggerFactory.getLogger(ArrivalStatsService.class);

    private static final String SCHEMA = "\"qixiao-apaas\".";

    /** MQTT 四闸站站名（报到 10min 窗/缺测 30min 窗，有效数据源 gate 表） */
    private static final Set<String> MQTT_SITE_NAMES = new HashSet<>(Arrays.asList(
            "南山寺节制闸", "渠首电站防洪闸", "渠首进水闸", "双庙湖节制闸"));

    /** 主监测要素业务表 */
    private static final String RIVER_TABLE = SCHEMA + "t_auto_hltgq_water_river_info";
    private static final String RAIN_TABLE  = SCHEMA + "t_auto_hltgq_water_rain_info";
    private static final String WT_TABLE    = SCHEMA + "t_auto_hltgq_water_wt_nfo";
    /** 墒情表（soilData）；nmisp_info/pcp_info 为水质表，水质站不参与缺测判定 */
    private static final String SOIL_TABLE = SCHEMA + "t_auto_hltgq_water_soil_data";
    private static final String GATE_TABLE  = SCHEMA + "t_auto_hltgq_water_gate";

    /** 采集状态统计维度（按大屏列表顺序；视频归 site/device） */
    private static final Map<String, String> COLLECT_DIMS = new LinkedHashMap<>();
    static {
        COLLECT_DIMS.put(RIVER_TABLE, "水位数据");
        COLLECT_DIMS.put(WT_TABLE,    "流量数据");
        COLLECT_DIMS.put(RAIN_TABLE,  "雨量数据");
        COLLECT_DIMS.put(GATE_TABLE,  "闸门开度");
        COLLECT_DIMS.put(SOIL_TABLE, "墒情数据");
    }

    /** 窗长(毫秒) */
    private static final long WINDOW_4H_MS    = 14400000L;
    private static final long WINDOW_1H_MS    = 3600000L;
    private static final long WINDOW_30MIN_MS = 1800000L;
    private static final long WINDOW_10MIN_MS = 600000L;
    private static final long DAY_MS          = 86400000L;

    /** 统计上下文短缓存 TTL（毫秒）：大屏高频轮询时避免重复聚合查询 */
    private static final long CACHE_TTL_MS = 60 * 1000L;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 本月统计起始日（报文流水上线日）可配置覆盖；默认空=自动取 msg_info 表 msg_text 非空首日。
     * 例：stats.start-date=2026-09-09
     */
    @Value("${stats.start-date:}")
    private String statsStartDateCfg;

    /**
     * 按 stcd 前缀整体排除的渠道站（可选，与产品口径对齐）。
     * 例：stats.exclude-stcd-prefix=9000000（召测渠道站不参与统计）
     */
    @Value("${stats.exclude-stcd-prefix:}")
    private String excludeStcdPrefixCfg;

    /** 统计上下文短缓存（volatile 双检，并发下重复加载无害仅浪费一次聚合） */
    private volatile long ctxTimeMs = 0;
    private volatile StatsContext cachedCtx = null;

    /** 站点统计信息 */
    private static class SiteInfo {
        String id;
        String name;
        String stcd;
        String epjutj;
        boolean mqtt;
        boolean rain;   // 雨量站：RTU 无雨 4h 一报（有雨加密），报到/缺测窗按 4h
    }

    /** 单站有效窗判定结果 */
    private static class ValidResult {
        Set<Long> valid = new HashSet<>();
        Timestamp lastValid = null;
        long mwin = WINDOW_1H_MS;
    }

    /** 统计共享上下文：一次加载供全部统计接口使用 */
    private static class StatsContext {
        LocalDate today;
        long todayStartMs;
        long monthStartMs;
        long nowMs;
        LocalDate statStartDate; // 流水上线日（可配置覆盖），用于月均窗口起点
        LocalDate effStartDate;  // 本月统计起始日 = max(statStartDate, 本月1日)，响应输出用
        List<SiteInfo> sites;
        Map<String, SiteInfo> siteById;
        List<Map<String, Object>> msgRows;
        Map<String, Map<String, Set<Long>>> validByTable;
        Map<String, Map<String, Timestamp>> lastValidByTable;
        Map<String, Set<Long>> arrivalWindows;      // siteId -> 今日到报窗桶
        Map<String, Set<Long>> rainArrivalPoints;   // siteId -> 今日 rainInfo 报文时间点
        Map<LocalDate, Map<String, Set<Long>>> dailyWindows; // 本月逐日到报聚合
    }

    // ==================== 共享上下文（带缓存） ====================

    private StatsContext getContext() {
        StatsContext ctx = cachedCtx;
        if (ctx != null && System.currentTimeMillis() - ctxTimeMs < CACHE_TTL_MS) {
            return ctx;
        }
        synchronized (this) {
            if (cachedCtx != null && System.currentTimeMillis() - ctxTimeMs < CACHE_TTL_MS) {
                return cachedCtx;
            }
            StatsContext c = loadContext();
            cachedCtx = c;
            ctxTimeMs = System.currentTimeMillis();
            return c;
        }
    }

    private StatsContext loadContext() {
        LocalDateTime nowLdt = LocalDateTime.now();
        StatsContext ctx = new StatsContext();
        ctx.today = nowLdt.toLocalDate();
        LocalDate monthStartDate = ctx.today.withDayOfMonth(1);
        ctx.todayStartMs = Timestamp.valueOf(ctx.today.atStartOfDay()).getTime();
        ctx.monthStartMs = Timestamp.valueOf(monthStartDate.atStartOfDay()).getTime();
        ctx.nowMs = Timestamp.valueOf(nowLdt).getTime();

        // === 统计起始日（流水上线日）：配置优先，否则取 msg_info 表 msg_text 非空首日 ===
        ctx.statStartDate = resolveStatStartDate(ctx);
        // 有效起始 = max(流水上线日, 本月1日)：跨月后统计窗口跟随本月，不回退到上线日之前
        LocalDate effStartDate = ctx.statStartDate.isBefore(monthStartDate) ? monthStartDate : ctx.statStartDate;
        ctx.effStartDate = effStartDate;
        long effStartMs = Timestamp.valueOf(effStartDate.atStartOfDay()).getTime();

        // === 参与站点（离线判定同口径：有stcd且有设备或本月流水的遥测站 + 有MQTT gate数据的闸站，剔除测试站） ===
        ctx.sites = querySites(ctx.monthStartMs);
        ctx.siteById = new HashMap<>();
        for (SiteInfo s : ctx.sites) {
            ctx.siteById.put(s.id, s);
        }

        // === 报文流水（有效起始日至今一次查出：今日到报 + 本月逐日到报共用） ===
        ctx.msgRows = jdbcTemplate.queryForList(
                "SELECT site, tm, msg FROM " + SCHEMA + "t_auto_hltgq_water_msg_info WHERE tm >= ?",
                new Timestamp(effStartMs));

        // === 有效数据（缺测判定）：按主监测要素聚合 table -> siteId -> 有效时间点集合 ===
        ctx.validByTable = new HashMap<>();
        ctx.lastValidByTable = new HashMap<>();
        loadValidWindows(ctx, RIVER_TABLE, "z > 0");
        loadValidWindows(ctx, RAIN_TABLE, "dyp > 0");
        loadValidWindows(ctx, WT_TABLE, "q >= 0");
        loadValidWindows(ctx, SOIL_TABLE, "mten >= 0");
        loadValidWindows(ctx, GATE_TABLE, "(up_z > 0 OR down_z > 0 OR open_degree >= 0)");

        // === 到报聚合 ===
        ctx.arrivalWindows = new HashMap<>();
        // rainInfo 报文时间点：雨量站晴天 DYP=0 不入库(守卫拦截)，但有报文即有效观测，
        // 否则晴天雨量站会被误判缺测（msg 列非 msgInfo tag 时存 tag 名）
        ctx.rainArrivalPoints = new HashMap<>();
        ctx.dailyWindows = new HashMap<>();
        for (Map<String, Object> row : ctx.msgRows) {
            Timestamp tm = toTimestamp(row.get("tm"));
            String siteId = String.valueOf(row.get("site"));
            if (tm == null || siteId == null || "null".equals(siteId)) continue;
            LocalDate day = tm.toLocalDateTime().toLocalDate();
            if (day.isBefore(effStartDate)) continue;
            SiteInfo s = ctx.siteById.get(siteId);
            if (s == null) continue;
            long win = arrivalWinMs(s);
            long bucket = tm.getTime() / win * win;
            // 当前进行中窗不计入：应报/应测分母只统计已结束完整窗，分子口径必须一致(防到报率超100%)
            long currentWinStart = ctx.todayStartMs + ((ctx.nowMs - ctx.todayStartMs) / win) * win;
            if (tm.getTime() >= ctx.todayStartMs && bucket >= currentWinStart) {
                continue;
            }
            if (tm.getTime() >= ctx.todayStartMs) {
                ctx.arrivalWindows.computeIfAbsent(siteId, k -> new HashSet<>()).add(bucket);
                if ("rainInfo".equals(String.valueOf(row.get("msg")))) {
                    ctx.rainArrivalPoints.computeIfAbsent(siteId, k -> new HashSet<>()).add(tm.getTime());
                }
            }
            ctx.dailyWindows.computeIfAbsent(day, k -> new HashMap<>())
                    .computeIfAbsent(siteId, k -> new HashSet<>()).add(bucket);
        }
        return ctx;
    }

    // ==================== 统计卡片聚合（已实现） ====================

    /**
     * 统计卡片：今日到报率/本月平均到报率/今日缺测率/未到报站点/缺测站点。
     */
    public Map<String, Object> getStats() {
        StatsContext ctx = getContext();

        long totalArrival = 0;   // 今日到报窗合计
        long totalExpected = 0;  // 今日应报窗合计
        long totalMiss = 0;      // 今日缺测窗合计
        long totalMeasured = 0;  // 今日应测窗合计（仅参与缺测判定的站点）
        List<Map<String, Object>> noReportSites = new ArrayList<>();
        List<Map<String, Object>> missedSites = new ArrayList<>();

        for (SiteInfo s : ctx.sites) {
            long win = arrivalWinMs(s);
            int expected = (int) ((ctx.nowMs - ctx.todayStartMs) / win); // 已结束的完整窗
            totalExpected += expected;

            Set<Long> aw = ctx.arrivalWindows.getOrDefault(s.id, Collections.emptySet());
            totalArrival += aw.size();

            if (aw.isEmpty()) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("siteId", s.id);
                item.put("siteName", s.name);
                item.put("stcd", s.stcd);
                noReportSites.add(item);
            }

            ValidResult vr = computeValid(ctx, s);
            if (vr == null) continue;
            int expectedM = (int) ((ctx.nowMs - ctx.todayStartMs) / vr.mwin);
            totalMeasured += expectedM;

            int missCount = 0;
            for (long w = ctx.todayStartMs; w + vr.mwin <= ctx.nowMs; w += vr.mwin) {
                if (!vr.valid.contains(w)) missCount++;
            }
            totalMiss += missCount;

            if (missCount > 0) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("siteId", s.id);
                item.put("siteName", s.name);
                item.put("stcd", s.stcd);
                item.put("lastValidTm", vr.lastValid != null ? vr.lastValid.toString() : null);
                item.put("missedWindows", missCount);
                item.put("missRate", round2(missCount * 100.0 / expectedM));
                missedSites.add(item);
            }
        }

        // === 本月平均到报率：从流水上线日起逐日平均 ===
        double monthAvg = 0;
        if (!ctx.dailyWindows.isEmpty()) {
            double sum = 0;
            int days = 0;
            for (Map.Entry<LocalDate, Map<String, Set<Long>>> entry : ctx.dailyWindows.entrySet()) {
                LocalDate d = entry.getKey();
                if (d.isBefore(ctx.effStartDate)) continue; // 防御：统计起始日之前的日不参与月均
                Map<String, Set<Long>> bySite = entry.getValue();
                long arrival = 0;
                long expected = 0;
                for (SiteInfo s : ctx.sites) {
                    long win = arrivalWinMs(s);
                    expected += d.isBefore(ctx.today) ? DAY_MS / win : (ctx.nowMs - ctx.todayStartMs) / win;
                    Set<Long> ws = bySite.get(s.id);
                    if (ws != null) arrival += ws.size();
                }
                if (expected > 0) {
                    sum += arrival * 100.0 / expected;
                    days++;
                }
            }
            monthAvg = days > 0 ? sum / days : 0;
        }

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("stationTotal", ctx.sites.size());
        data.put("todayArrivalRate", round2(totalExpected > 0 ? totalArrival * 100.0 / totalExpected : 0));
        data.put("monthAvgArrivalRate", round2(monthAvg));
        data.put("todayMissRate", round2(totalMeasured > 0 ? totalMiss * 100.0 / totalMeasured : 0));
        data.put("statStartDate", ctx.effStartDate != null ? ctx.effStartDate.toString() : ctx.today.toString());
        data.put("noReportSites", noReportSites);
        data.put("missedSites", missedSites);
        return data;
    }

    // ==================== 站点到报明细 ====================

    /**
     * 站点到报明细：每站一行。
     * 字段：siteId/siteName/stcd/msgType(整点报|分钟报)/expected/arrived/missed/arrivalRate。
     * 状态档位（正常/偏低/异常/未到报）阈值由产品定义，此处给数字。
     */
    public List<Map<String, Object>> getArrivalDetail() {
        StatsContext ctx = getContext();
        List<Map<String, Object>> list = new ArrayList<>();
        for (SiteInfo s : ctx.sites) {
            long win = arrivalWinMs(s);
            int expected = (int) ((ctx.nowMs - ctx.todayStartMs) / win);
            int arrived = ctx.arrivalWindows.getOrDefault(s.id, Collections.emptySet()).size();
            int missed = expected - arrived;

            Map<String, Object> item = new LinkedHashMap<>();
            item.put("siteId", s.id);
            item.put("siteName", s.name);
            item.put("stcd", s.stcd);
            item.put("msgType", s.mqtt ? "分钟报" : "整点报");
            item.put("expected", expected);
            item.put("arrived", arrived);
            item.put("missed", missed);
            item.put("arrivalRate", round2(expected > 0 ? arrived * 100.0 / expected : 0));
            list.add(item);
        }
        return list;
    }

    // ==================== 缺测明细 ====================

    /**
     * 缺测明细：按缺测时间段展开，连续缺测窗合并为一段。
     * 字段：siteId/siteName/stcd/startTm/endTm/missMinutes/dataTypes/status(缺测中|已恢复)。
     */
    public List<Map<String, Object>> getMissDetail() {
        StatsContext ctx = getContext();
        List<Map<String, Object>> list = new ArrayList<>();

        // 最后一个已结束完整窗的起点：缺测段覆盖它即为"缺测中"，否则"已恢复"
        for (SiteInfo s : ctx.sites) {
            ValidResult vr = computeValid(ctx, s);
            if (vr == null) continue;
            long mwin = vr.mwin;
            long lastCompleteStart = ctx.todayStartMs
                    + ((ctx.nowMs - ctx.todayStartMs) / mwin - 1) * mwin;

            // 收集缺测窗并按连续合并为段
            List<long[]> segments = new ArrayList<>();
            Long segStart = null;
            long segEnd = 0;
            for (long w = ctx.todayStartMs; w + mwin <= ctx.nowMs; w += mwin) {
                if (!vr.valid.contains(w)) {
                    if (segStart == null) {
                        segStart = w;
                        segEnd = w + mwin;
                    } else if (w == segEnd) {
                        segEnd = w + mwin;
                    } else {
                        segments.add(new long[]{segStart, segEnd});
                        segStart = w;
                        segEnd = w + mwin;
                    }
                }
            }
            if (segStart != null) {
                segments.add(new long[]{segStart, segEnd});
            }

            for (long[] seg : segments) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("siteId", s.id);
                item.put("siteName", s.name);
                item.put("stcd", s.stcd);
                item.put("startTm", new Timestamp(seg[0]).toString());
                item.put("endTm", new Timestamp(seg[1]).toString());
                item.put("missMinutes", (seg[1] - seg[0]) / 60000L);
                item.put("dataTypes", typeNames(s));
                // 缺测中：段覆盖最后一个完整窗；已恢复：之后已有有效数据
                item.put("status", (seg[1] - mwin) >= lastCompleteStart ? "缺测中" : "已恢复");
                list.add(item);
            }
        }
        return list;
    }

    // ==================== 数据采集状态统计 ====================

    /**
     * 采集状态统计：按数据类型（水位/流量/雨量/闸门开度/墒情）聚合。
     * 采集周期=缺测窗（RabbitMQ 1h / MQTT 30min）。
     * expected=应采窗 / collected=实采窗(有报文) / success=成功窗(有效数据入库)
     * failed=实采-成功(有报文但数据无效) / successRate/failRate 按应采计。
     */
    public List<Map<String, Object>> getCollectStats() {
        StatsContext ctx = getContext();
        List<Map<String, Object>> list = new ArrayList<>();

        for (Map.Entry<String, String> dim : COLLECT_DIMS.entrySet()) {
            String table = dim.getKey();
            long expected = 0;
            long collected = 0;
            long success = 0;
            for (SiteInfo s : ctx.sites) {
                Set<String> primary = primaryTables(s);
                if (!primary.contains(table)) continue;
                long mwin = measureWinMs(s);
                // 进行中窗不计入（分母只统计已结束完整窗）
                long currentWinStart = ctx.todayStartMs + ((ctx.nowMs - ctx.todayStartMs) / mwin) * mwin;
                expected += (ctx.nowMs - ctx.todayStartMs) / mwin;

                // 实采：到报窗换算到采集周期桶
                Set<Long> aw = ctx.arrivalWindows.getOrDefault(s.id, Collections.emptySet());
                Set<Long> collectedWin = new HashSet<>();
                for (Long b : aw) {
                    long bucket = b / mwin * mwin;
                    if (bucket >= currentWinStart) continue;
                    collectedWin.add(bucket);
                }

                // 成功：该维度表有效窗；雨量维度并入 rainInfo 报文窗（晴天 DYP=0 为有效观测）
                Set<Long> validWin = new HashSet<>();
                Set<Long> ws = ctx.validByTable.getOrDefault(table, Collections.emptyMap()).get(s.id);
                if (ws != null) {
                    for (Long t : ws) {
                        long bucket = t / mwin * mwin;
                        if (bucket >= currentWinStart) continue;
                        validWin.add(bucket);
                    }
                }
                if (RAIN_TABLE.equals(table)) {
                    Set<Long> rp = ctx.rainArrivalPoints.get(s.id);
                    if (rp != null) {
                        for (Long t : rp) {
                            long bucket = t / mwin * mwin;
                            if (bucket >= currentWinStart) continue;
                            validWin.add(bucket);
                        }
                    }
                }
                // 成功=有报文且有有效数据的窗（交集口径）：保证 collected=success+failed 恒成立、success<=collected
                // 跨桶分钟偏差（报文 tm 与业务行 tm 相差几分钟）归入 failed，不产生"实采<成功"矛盾
                collected += collectedWin.size();
                validWin.retainAll(collectedWin);
                success += validWin.size();
            }
            long failed = collected - success;

            Map<String, Object> item = new LinkedHashMap<>();
            item.put("dataType", dim.getValue());
            item.put("expected", expected);
            item.put("collected", collected);
            item.put("success", success);
            item.put("failed", failed);
            item.put("successRate", round2(expected > 0 ? success * 100.0 / expected : 0));
            item.put("failRate", round2(expected > 0 ? failed * 100.0 / expected : 0));
            list.add(item);
        }
        return list;
    }

    // ==================== 服务监测（mq 自身） ====================

    /**
     * mq 自身服务状态：进程指标 + 接收/解析/存储三个逻辑服务的今日处理量与成功率。
     * 信息发布/告警推送服务状态由 hltgq-site 提供。
     */
    public Map<String, Object> getServiceStatus() {
        StatsContext ctx = getContext();

        // === 进程指标（三逻辑服务同进程） ===
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        long startMs = runtimeBean.getStartTime();
        long uptimeMinutes = (System.currentTimeMillis() - startMs) / 60000L;
        Runtime runtime = Runtime.getRuntime();
        long usedMemMb = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
        long maxMemMb = runtime.maxMemory() / (1024 * 1024);
        double cpu = 0;
        try {
            OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
            double load = osBean.getProcessCpuLoad();
            if (load > 0) {
                cpu = load;
            }
        } catch (Exception e) {
            log.debug("获取进程 CPU 负载失败: {}", e.getMessage());
        }

        // === 今日接收/解析报文数（msg_info 行数，含 MQTT 一条 payload 按 4 站拆 4 行） ===
        long todayRequests = 0;
        for (Map<String, Object> row : ctx.msgRows) {
            Timestamp tm = toTimestamp(row.get("tm"));
            if (tm != null && tm.getTime() >= ctx.todayStartMs) {
                todayRequests++;
            }
        }

        // === 今日入库行数（各业务表合计，逐表容错：单表失败不影响其他表） ===
        long todayStored = 0;
        Timestamp ts = new Timestamp(ctx.todayStartMs);
        // pcp_info 无 tm 列(历史结构)，以 spt 采样时间列统计今日行数
        Map<String, String> storeTables = new LinkedHashMap<>();
        storeTables.put(RIVER_TABLE, "tm");
        storeTables.put(RAIN_TABLE, "tm");
        storeTables.put(WT_TABLE, "tm");
        storeTables.put(SOIL_TABLE, "tm");
        storeTables.put(GATE_TABLE, "tm");
        storeTables.put(SCHEMA + "t_auto_hltgq_water_vol_info", "tm");
        storeTables.put(SCHEMA + "t_auto_hltgq_water_sluice_discharge", "tm");
        storeTables.put(SCHEMA + "t_auto_hltgq_water_nmisp_info", "tm");
        storeTables.put(SCHEMA + "t_auto_hltgq_water_pcp_info", "spt");
        for (Map.Entry<String, String> e : storeTables.entrySet()) {
            try {
                String sql = "SELECT COUNT(*) FROM " + e.getKey() + " WHERE " + e.getValue() + " >= ?";
                Number n = jdbcTemplate.queryForObject(sql, Number.class, ts);
                todayStored += n != null ? n.longValue() : 0;
            } catch (Exception ex) {
                log.warn("统计今日入库行数失败(表{}不计入): {}", e.getKey(), ex.getMessage());
            }
        }

        // === 存储成功率：有报文且有效入库的窗 / 有报文的窗（交集口径，<=100%） ===
        long validSum = 0;
        long collectedSum = 0;
        for (SiteInfo s : ctx.sites) {
            ValidResult vr = computeValid(ctx, s);
            if (vr == null) continue;
            long mwin = vr.mwin;
            // 进行中窗不计入（分母只统计已结束完整窗）
            long currentWinStart = ctx.todayStartMs + ((ctx.nowMs - ctx.todayStartMs) / mwin) * mwin;
            Set<Long> aw = ctx.arrivalWindows.getOrDefault(s.id, Collections.emptySet());
            Set<Long> collectedWin = new HashSet<>();
            for (Long b : aw) {
                long bucket = b / mwin * mwin;
                if (bucket >= currentWinStart) continue;
                collectedWin.add(bucket);
            }
            Set<Long> validIntersect = new HashSet<>(vr.valid);
            validIntersect.retainAll(collectedWin);
            validSum += validIntersect.size();
            collectedSum += collectedWin.size();
        }
        double storeRate = round2(collectedSum > 0 ? validSum * 100.0 / collectedSum : 0);

        // === 组装三个逻辑服务 ===
        Map<String, Object> process = new LinkedHashMap<>();
        process.put("startTime", new Timestamp(startMs).toString());
        process.put("uptimeMinutes", uptimeMinutes);
        process.put("cpu", round2(cpu * 100));       // 百分比
        process.put("memoryUsedMB", usedMemMb);
        process.put("memoryMaxMB", maxMemMb);

        List<Map<String, Object>> services = new ArrayList<>();
        // 数据接收服务
        Map<String, Object> receive = new LinkedHashMap<>();
        receive.put("name", "数据接收服务");
        receive.put("status", "运行中");
        receive.put("todayRequests", todayRequests);
        receive.put("successRate", 100.0); // MQ 至少一次投递语义（auto-ack，未确认消息重投），无失败计数
        services.add(receive);
        // 数据解析服务
        Map<String, Object> parse = new LinkedHashMap<>();
        parse.put("name", "数据解析服务");
        parse.put("status", "运行中");
        parse.put("todayRequests", todayRequests);
        parse.put("successRate", 100.0); // 解析失败仅 WARN 日志留痕未埋点计数，此处按接收总量计
        services.add(parse);
        // 数据存储服务
        Map<String, Object> store = new LinkedHashMap<>();
        store.put("name", "数据存储服务");
        store.put("status", "运行中");
        store.put("todayRequests", todayStored);
        store.put("successRate", storeRate);
        services.add(store);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("process", process);
        data.put("services", services);
        return data;
    }

    // ==================== 内部辅助 ====================

    /**
     * 站点到报窗长：雨量站 4h（RTU 无雨 4h 一报、有雨加密）；MQTT 闸站 10min；其余 RabbitMQ 站 1h。
     */
    private long arrivalWinMs(SiteInfo s) {
        if (s.rain) return WINDOW_4H_MS;
        return s.mqtt ? WINDOW_10MIN_MS : WINDOW_1H_MS;
    }

    /**
     * 站点采集周期（缺测窗）长：雨量站 4h；MQTT 闸站 30min（对齐 gate 批量入库）；其余 1h。
     */
    private long measureWinMs(SiteInfo s) {
        if (s.rain) return WINDOW_4H_MS;
        return s.mqtt ? WINDOW_30MIN_MS : WINDOW_1H_MS;
    }

    /** 计算站点今日有效窗集合(按缺测窗桶对齐)；无主监测要素映射返回 null(不参与缺测) */
    private ValidResult computeValid(StatsContext ctx, SiteInfo s) {
        Set<String> primary = primaryTables(s);
        if (primary.isEmpty()) return null;
        long mwin = measureWinMs(s);
        // 进行中窗不计入（分母只统计已结束完整窗）
        long currentWinStart = ctx.todayStartMs + ((ctx.nowMs - ctx.todayStartMs) / mwin) * mwin;

        ValidResult vr = new ValidResult();
        vr.mwin = mwin;
        for (String table : primary) {
            Map<String, Set<Long>> bySite = ctx.validByTable.getOrDefault(table, Collections.emptyMap());
            Set<Long> ws = bySite.get(s.id);
            if (ws != null) {
                for (Long t : ws) {
                    long bucket = t / mwin * mwin;
                    if (bucket >= currentWinStart) continue;
                    vr.valid.add(bucket);
                }
            }
            Timestamp lt = ctx.lastValidByTable.getOrDefault(table, Collections.emptyMap()).get(s.id);
            if (lt != null && (vr.lastValid == null || lt.after(vr.lastValid))) {
                vr.lastValid = lt;
            }
            // 雨量站：晴天 DYP=0 不入库但有 rainInfo 报文，视为有效观测
            if (RAIN_TABLE.equals(table)) {
                Set<Long> rp = ctx.rainArrivalPoints.get(s.id);
                if (rp != null) {
                    long maxT = 0;
                    for (Long t : rp) {
                        long bucket = t / mwin * mwin;
                        if (bucket >= currentWinStart) continue;
                        vr.valid.add(bucket);
                        if (t > maxT) maxT = t;
                    }
                    if (vr.lastValid == null || maxT > vr.lastValid.getTime()) {
                        vr.lastValid = new Timestamp(maxT);
                    }
                }
            }
        }
        return vr;
    }

    /** 站点主监测要素类型名列表（缺测明细 dataTypes 用） */
    private List<String> typeNames(SiteInfo s) {
        List<String> names = new ArrayList<>();
        if (s.mqtt) {
            names.add("闸门开度");
            return names;
        }
        if (s.epjutj == null || s.epjutj.isEmpty()) {
            return names;
        }
        if (s.epjutj.contains("#1#")) names.add("水位");
        if (s.epjutj.contains("#2#")) names.add("雨量");
        if (s.epjutj.contains("#3#")) names.add("流量");
        if (s.epjutj.contains("#4#")) names.add("闸门开度");
        if (s.epjutj.contains("#7#")) names.add("墒情");
        return names;
    }

    /**
     * 统计起始日（流水上线日）：配置 stats.start-date 优先；否则取 msg_info 表 msg_text 非空首日；兜底今天。
     */
    private LocalDate resolveStatStartDate(StatsContext ctx) {
        if (statsStartDateCfg != null && !statsStartDateCfg.trim().isEmpty()) {
            try {
                LocalDate d = LocalDate.parse(statsStartDateCfg.trim());
                log.info("统计起始日使用配置 stats.start-date={}", d);
                return d;
            } catch (Exception e) {
                log.warn("stats.start-date 配置格式非法, 回退自动探测: {}", statsStartDateCfg);
            }
        }
        try {
            String sql = "SELECT MIN(tm) FROM " + SCHEMA + "t_auto_hltgq_water_msg_info WHERE msg_text IS NOT NULL";
            Timestamp first = jdbcTemplate.queryForObject(sql, Timestamp.class);
            if (first != null) {
                LocalDate d = first.toLocalDateTime().toLocalDate();
                log.info("统计起始日自动探测(流水 msg_text 非空首日)={}", d);
                return d;
            }
        } catch (Exception e) {
            log.warn("探测流水上线日失败, 回退今天: {}", e.getMessage());
        }
        return ctx.today;
    }

    /**
     * 查询参与统计的站点：有stcd的遥测站须"有设备档案 或 本月有报文流水"，
     * 防止新建档从未接设备的渠道站(9000000xxx)计入应报拉低到报率；MQTT闸站有gate数据即参与；剔除测试站。
     */
    private List<SiteInfo> querySites(long monthStartTs) {
        String sql = "SELECT s.id, s.zzkaec, s.iofhpi, s.epjutj FROM " + SCHEMA + "t_auto_hltgq_5nw74_vnqqef s " +
                "WHERE (s.iofhpi IS NOT NULL AND (" +
                "   EXISTS (SELECT 1 FROM " + SCHEMA + "t_auto_hltgq_water_device d WHERE d.site = s.id)" +
                "   OR EXISTS (SELECT 1 FROM " + SCHEMA + "t_auto_hltgq_water_msg_info m " +
                "        WHERE m.site = s.id AND m.tm >= ?))) " +
                "   OR EXISTS (SELECT 1 FROM " + GATE_TABLE + " g WHERE g.site = s.id)";
        List<SiteInfo> result = new ArrayList<>();
        for (Map<String, Object> row : jdbcTemplate.queryForList(sql, new Timestamp(monthStartTs))) {
            SiteInfo s = new SiteInfo();
            s.id = String.valueOf(row.get("id"));
            s.name = row.get("zzkaec") != null ? String.valueOf(row.get("zzkaec")) : s.id;
            s.stcd = row.get("iofhpi") != null ? String.valueOf(row.get("iofhpi")) : null;
            s.epjutj = row.get("epjutj") != null ? String.valueOf(row.get("epjutj")) : null;
            s.mqtt = MQTT_SITE_NAMES.contains(s.name);
            s.rain = s.epjutj != null && s.epjutj.contains("#2#");
            // 剔除测试站（站名含"测试"或 stcd 为 9999 测试码段），不参与统计（与业务口径对齐）
            boolean testSite = (s.name != null && s.name.contains("测试"))
                    || (s.stcd != null && s.stcd.startsWith("9999"));
            if (testSite) {
                log.info("统计剔除测试站: id={}, name={}, stcd={}", s.id, s.name, s.stcd);
                continue;
            }
            // 渠道前缀站整体排除（可选配置）：召测渠道站等非正式上报站不计应报，避免拉低到报率
            if (excludeStcdPrefixCfg != null && !excludeStcdPrefixCfg.trim().isEmpty()
                    && s.stcd != null && s.stcd.startsWith(excludeStcdPrefixCfg.trim())) {
                log.info("统计剔除渠道前缀站(stats.exclude-stcd-prefix={}): id={}, name={}, stcd={}",
                        excludeStcdPrefixCfg.trim(), s.id, s.name, s.stcd);
                continue;
            }
            result.add(s);
        }
        log.info("统计参与站点 {} 个(已剔除测试站与无设备无流水站点)", result.size());
        return result;
    }

    /** 查询业务表今日有效行，聚合 siteId -> 有效时间点集合，并记录各站最后有效时间 */
    private void loadValidWindows(StatsContext ctx, String tableName, String validCond) {
        try {
            String sql = "SELECT site, tm FROM " + tableName + " WHERE tm >= ? AND " + validCond;
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, new Timestamp(ctx.todayStartMs));
            Map<String, Set<Long>> bySite = new HashMap<>();
            Map<String, Timestamp> lastBySite = new HashMap<>();
            for (Map<String, Object> row : rows) {
                Timestamp tm = toTimestamp(row.get("tm"));
                String siteId = row.get("site") != null ? String.valueOf(row.get("site")) : null;
                if (tm == null || siteId == null || "null".equals(siteId)) continue;
                bySite.computeIfAbsent(siteId, k -> new HashSet<>()).add(tm.getTime());
                Timestamp last = lastBySite.get(siteId);
                if (last == null || tm.after(last)) {
                    lastBySite.put(siteId, tm);
                }
            }
            ctx.validByTable.put(tableName, bySite);
            ctx.lastValidByTable.put(tableName, lastBySite);
        } catch (Exception e) {
            log.warn("查询有效数据失败(该表可能缺列), table={}: {}", tableName, e.getMessage());
            ctx.validByTable.put(tableName, Collections.emptyMap());
            ctx.lastValidByTable.put(tableName, Collections.emptyMap());
        }
    }

    /** 站点主监测要素表集合（按 epjutj 类型字典映射）；无类型返回空集(不参与缺测) */
    private Set<String> primaryTables(SiteInfo s) {
        if (s.mqtt) {
            return Collections.singleton(GATE_TABLE);
        }
        if (s.epjutj == null || s.epjutj.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> tables = new LinkedHashSet<>();
        if (s.epjutj.contains("#1#")) tables.add(RIVER_TABLE);
        if (s.epjutj.contains("#2#")) tables.add(RAIN_TABLE);
        if (s.epjutj.contains("#3#")) tables.add(WT_TABLE);
        if (s.epjutj.contains("#4#")) tables.add(GATE_TABLE);
        if (s.epjutj.contains("#7#")) tables.add(SOIL_TABLE);
        return tables;
    }

    private Timestamp toTimestamp(Object value) {
        if (value == null) return null;
        if (value instanceof Timestamp) return (Timestamp) value;
        if (value instanceof java.util.Date) return new Timestamp(((java.util.Date) value).getTime());
        try {
            return Timestamp.valueOf(String.valueOf(value));
        } catch (Exception e) {
            return null;
        }
    }

    private double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
