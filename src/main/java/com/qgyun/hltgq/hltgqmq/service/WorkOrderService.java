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
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 工单服务：告警触发时自动生成工单（t_auto_hltgq_water_work_order）。
 * <p>
 * 规则（与平台约定）：
 * - 三类告警（设备异常/站点失联/阈值越界）新增时同步生成工单，status=#1# 待处理；
 * - 同一 site+device+title 的未关闭工单（status 非 #3#/#4#）不重复生成，与告警去重同构；
 * - alert 字段存告警ID，形成工单↔告警精确关联（平台可按告警 type 区分工单类别，工单表无需 type）；
 * - 告警恢复自动关闭时同步关闭对应工单（status=#3# 已关闭），形成自动闭环；
 * - org 固定存部门 ID：库上站点 → 库上防汛办，其余站点 → 库下防汛办
 *   （org 表按 code 00000003/00000004 解析 ID，启动时加载）；
 * - user/time 暂留空（处理人员待平台指派，完成时限等后续需求）。
 */
@Service
public class WorkOrderService {

    private static final Logger log = LoggerFactory.getLogger(WorkOrderService.class);

    /** 人大金仓 schema（带双引号，因为含连字符） */
    private static final String SCHEMA = "\"qixiao-apaas\".";

    private static final String WORK_ORDER_TABLE = SCHEMA + "t_auto_hltgq_water_work_order";
    private static final String ORG_TABLE        = SCHEMA + "t_apaas_uc_org";
    private static final String SITE_TABLE       = SCHEMA + "t_auto_hltgq_5nw74_vnqqef";

    /** 部门 code 常量：库上防汛办 / 库下防汛办 */
    private static final String ORG_UP_CODE   = "00000003";
    private static final String ORG_DOWN_CODE = "00000004";

    /** 工单状态字典：#1#待处理 #2#处理中 #3#已关闭 #4#已取消；自动生成默认 #1#，自动闭环置 #3# */
    private static final String STATUS_PENDING = "#1#";
    private static final String STATUS_CLOSED  = "#3#";
    private static final String STATUS_CANCELED = "#4#";

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${app.corp-code:hltgq}")
    private String corpCode;

    /** 库上站点清单（stcd 逗号分隔，其余站点均按库下处理） */
    @Value("${work-order.up-stcd:}")
    private String upStcdConfig;

    /** 库上站点 stcd 集合（大写归一） */
    private Set<String> upStcdSet = Collections.emptySet();

    /** 工单表有效列名缓存（动态列适配，与各入库表一致） */
    private Set<String> workOrderColumns = Collections.emptySet();

    /** 部门 ID 解析结果：code → org 表 id（启动时加载一次，部门数据稳定） */
    private volatile String upOrgId;
    private volatile String downOrgId;

    /** siteId → iofhpi(stcd) 缓存（站点站码基本不变，永久缓存） */
    private final ConcurrentMap<String, String> siteStcdCache = new ConcurrentHashMap<>();

    /** 工单编号序号：按秒重置、同秒递增（code=GD+yyyyMMddHHmmss+3位序号） */
    private final AtomicInteger codeSeq = new AtomicInteger();
    private volatile long codeLastSecond = 0;

    private static final DateTimeFormatter CODE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    @PostConstruct
    public void init() {
        // 1. 工单表列名元数据（列不存在时自动跳过，动态列适配）
        try {
            String sql = "SELECT column_name FROM information_schema.columns " +
                    "WHERE table_schema = 'qixiao-apaas' AND table_name = 't_auto_hltgq_water_work_order'";
            List<String> cols = jdbcTemplate.queryForList(sql, String.class);
            workOrderColumns = new HashSet<>();
            for (String col : cols) {
                workOrderColumns.add(col.toLowerCase());
            }
            log.info("已加载工单表列名元数据: {} 列", workOrderColumns.size());
        } catch (Exception e) {
            log.error("加载工单表列名元数据失败", e);
        }
        // 2. 库上站点清单
        Set<String> set = new HashSet<>();
        if (upStcdConfig != null && !upStcdConfig.trim().isEmpty()) {
            for (String s : upStcdConfig.split(",")) {
                if (s.trim().isEmpty()) continue;
                set.add(s.trim().toUpperCase());
            }
        }
        upStcdSet = set;
        log.info("工单库上站点清单 {} 个: {}", upStcdSet.size(), upStcdSet);
        // 3. 部门 ID 解析
        loadOrgIds();
    }

    /** 按 code 解析库上/库下防汛办的部门 ID（org 固定存 ID，平台架构约定） */
    private void loadOrgIds() {
        try {
            String sql = "SELECT id, code FROM " + ORG_TABLE +
                    " WHERE corp_code = ? AND code IN (?, ?)";
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql,
                    corpCode, ORG_UP_CODE, ORG_DOWN_CODE);
            for (Map<String, Object> row : rows) {
                String code = String.valueOf(row.get("code")).trim();
                String id = String.valueOf(row.get("id"));
                if (ORG_UP_CODE.equals(code)) upOrgId = id;
                if (ORG_DOWN_CODE.equals(code)) downOrgId = id;
            }
            if (upOrgId == null) {
                log.error("!!!!! 未解析到库上防汛办部门ID(code=00000003)，库上站点工单org将留空 !!!!!");
            }
            if (downOrgId == null) {
                log.error("!!!!! 未解析到库下防汛办部门ID(code=00000004)，库下站点工单org将留空 !!!!!");
            }
            log.info("工单负责部门ID: 库上防汛办={}, 库下防汛办={}", upOrgId, downOrgId);
        } catch (Exception e) {
            log.error("解析防汛办部门ID失败", e);
        }
    }

    // ======================== 工单生成 ========================

    /**
     * 告警新增成功后同步生成工单（告警去重已通过，此处再做工单侧去重双保险）。
     * alert 字段存告警ID精确关联；title 由告警 content 派生（去结尾"！"），
     * content 与告警 content 一致，关闭时按 content 匹配（与告警关闭条件同构）。
     */
    public void createIfAbsent(String alertId, String siteId, String deviceId, String title, String content) {
        if (siteId == null || title == null) return;
        if (existsUnclosed(siteId, deviceId, title)) {
            return;
        }
        String orgId = resolveOrg(siteId);
        if (orgId == null) {
            log.warn("工单负责部门未解析, org留空: site={}, title={}", siteId, title);
        }
        insertWorkOrder(alertId, siteId, deviceId, title, content, orgId);
    }

    /** 同一 site+device+title 的未关闭工单（非 #3#已关闭/#4#已取消）是否存在 */
    private boolean existsUnclosed(String siteId, String deviceId, String title) {
        try {
            String sql = "SELECT COUNT(*) FROM " + WORK_ORDER_TABLE +
                    " WHERE site = ? AND device IS NOT DISTINCT FROM ? AND title = ? " +
                    " AND status IS DISTINCT FROM ? AND status IS DISTINCT FROM ?";
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class,
                    siteId, deviceId, title, STATUS_CLOSED, STATUS_CANCELED);
            return count != null && count > 0;
        } catch (Exception e) {
            log.debug("工单去重检查失败, 放行: {}", e.getMessage());
            return false;
        }
    }

    /** 站点归属部门：库上清单内 → 库上防汛办；其余（含查不到站码）→ 库下防汛办 */
    private String resolveOrg(String siteId) {
        String stcd = siteStcdCache.computeIfAbsent(siteId, id -> {
            try {
                String sql = "SELECT iofhpi FROM " + SITE_TABLE + " WHERE id = ?";
                List<String> rows = jdbcTemplate.queryForList(sql, String.class, id);
                return rows.isEmpty() || rows.get(0) == null ? "" : rows.get(0).trim();
            } catch (Exception e) {
                log.debug("查询站点站码失败, site={}: {}", id, e.getMessage());
                return "";
            }
        });
        if (!stcd.isEmpty() && upStcdSet.contains(stcd.toUpperCase())) {
            return upOrgId;
        }
        return downOrgId;
    }

    /** 工单入库：alert 存告警ID，status=#1# 待处理，user/time/result/file 留空 */
    private void insertWorkOrder(String alertId, String siteId, String deviceId, String title, String content, String orgId) {
        try {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            Map<String, Object> fm = new LinkedHashMap<>();
            fm.put("id",         IdGenerator.generate());
            fm.put("corp_code",  corpCode);
            fm.put("created_at", now);
            fm.put("created_by", "SYSTEM");
            fm.put("updated_at", now);
            fm.put("updated_by", "SYSTEM");
            fm.put("code",       genWorkOrderCode(now));
            fm.put("title",      title);
            fm.put("content",    content);
            fm.put("site",       siteId);
            if (deviceId != null) fm.put("device", deviceId);
            if (alertId != null)  fm.put("alert", alertId);
            if (orgId != null)    fm.put("org", orgId);
            fm.put("status",     STATUS_PENDING);

            StringBuilder cols = new StringBuilder();
            StringBuilder phs = new StringBuilder();
            List<Object> vals = new ArrayList<>();
            for (Map.Entry<String, Object> e : fm.entrySet()) {
                if (!workOrderColumns.isEmpty() && !workOrderColumns.contains(e.getKey().toLowerCase())) {
                    continue; // 列不存在则跳过（动态列适配）
                }
                if (cols.length() > 0) { cols.append(", "); phs.append(", "); }
                // org/user 等可能为平台方言保留字，加双引号保险（其余列名与库内小写列名一致）
                String col = "org".equalsIgnoreCase(e.getKey()) ? "\"org\"" : e.getKey();
                cols.append(col);
                phs.append("?");
                vals.add(e.getValue());
            }
            String sql = String.format("INSERT INTO %s (%s) VALUES (%s)", WORK_ORDER_TABLE, cols, phs);
            jdbcTemplate.update(sql, vals.toArray());
            log.info("告警触发自动生成工单: code={}, site={}, device={}, title={}, org={}",
                    fm.get("code"), siteId, deviceId, title, orgId);
        } catch (Exception e) {
            log.error("工单入库失败, site={}, title={}: {}", siteId, title, e.getMessage());
        }
    }

    /**
     * 工单编号：GD + yyyyMMddHHmmss + 3位序号（按秒重置、同秒递增）。
     * 应用重启后同秒序号可能撞车，概率极低且 code 无唯一约束时无害。
     */
    private String genWorkOrderCode(Timestamp tm) {
        long sec = tm.getTime() / 1000;
        int seq;
        synchronized (codeSeq) {
            if (sec != codeLastSecond) {
                codeLastSecond = sec;
                codeSeq.set(0);
            }
            seq = codeSeq.incrementAndGet();
        }
        return "GD" + tm.toLocalDateTime().format(CODE_TIME_FORMATTER) + String.format("%03d", seq);
    }

    // ======================== 告警恢复联动关闭 ========================

    /** 设备异常恢复：按 content 精确匹配关闭工单（与告警 closeByContent 同条件） */
    public void closeByContent(String siteId, String deviceId, String content) {
        updateClose(" WHERE site = ? AND device IS NOT DISTINCT FROM ? AND content = ?",
                siteId, deviceId, content);
    }

    /** 站点恢复通信：关闭该站失联类工单（content LIKE pattern） */
    public void closeByLike(String siteId, String pattern) {
        updateClose(" WHERE site = ? AND content LIKE ?", siteId, pattern);
    }

    /** 指标恢复正常：关闭该站该设备该指标的阈值类工单（与告警 closeThresholdAlerts 同条件） */
    public void closeThreshold(String siteId, String deviceId, String prefix, String extraCond) {
        updateClose(" WHERE site = ? AND device IS NOT DISTINCT FROM ? AND content LIKE ? " + extraCond,
                siteId, deviceId, prefix);
    }

    /** 工单自动闭环：置 status=#3# 已关闭（告警恢复时由 AlertService 调用） */
    private void updateClose(String whereSql, Object... params) {
        try {
            Timestamp now = new Timestamp(System.currentTimeMillis());
            List<Object> vals = new ArrayList<>();
            vals.add(STATUS_CLOSED);
            vals.add(now);
            vals.addAll(Arrays.asList(params));
            vals.add(STATUS_CLOSED);
            String sql = "UPDATE " + WORK_ORDER_TABLE +
                    " SET status = ?, updated_at = ?, updated_by = 'SYSTEM' " +
                    whereSql + " AND status IS DISTINCT FROM ?";
            int rows = jdbcTemplate.update(sql, vals.toArray());
            if (rows > 0) {
                log.info("告警恢复, 自动关闭工单 {} 条", rows);
            }
        } catch (Exception e) {
            log.debug("关闭工单失败: {}", e.getMessage());
        }
    }
}
