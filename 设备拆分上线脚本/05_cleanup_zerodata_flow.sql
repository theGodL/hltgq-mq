-- 设备拆分上线：⑤ 补充清理——零数据"流量计#"设备（wtInfo 守卫缺口同类产物）
-- 生成：2026-10-26 晚；依据：上线+04 清理后的全面核对（DeployVerify/DeployVerify2）
--
-- 背景：
--   04 清理按 site 质疑口径只覆盖了水质监测仪#/雨量计#/闸孔1# 三类。上线后核查发现
--   流量计# 也存在同类产物：wtInfo 报文的 Q 缺失/异常守卫（"wtInfo 缺少流量字段Q"/"瞬时流量异常"）
--   在设备解析之后 return，Q 无效的报文每轮都会创建/标在线"流量计#"设备但不写 wt_nfo。
--   当前全库 11 台零数据流量计#（10 台 2026-10-26 15:40~16:37 创建 + 1 台 2026-08-16 历史）。
--   mq 侧已扩充 willBeGuardedOut 收口 wtInfo/riverInfo 两类守卫（第二批代码），
--   部署后不再产生此类设备；本脚本清理存量。
--
-- !! 执行顺序 !!
--   1. 必须先部署第二批收口代码（willBeGuardedOut 扩充 wtInfo/riverInfo）并重启，
--      否则删掉的设备会被仍运行的代码在下一轮 Q 无效报文中重新创建；
--   2. 单事务执行，先看"核对清单"输出（预期 11 台），偏差过大时人工中止后再 COMMIT；
--   3. 005 与 006 的断链口径与 04 完全一致：告警/工单内容与状态保留，仅断开设备引用。
--
-- 判定口径：
--   * 名称以"流量计#"结尾且 type 含 #3#，且 7 张业务表（wt_nfo/river_info/rain_info/gate/
--     soil_data/nmisp_info/pcp_info）全零；msg/vol 留痕流水不计。
--   * 无 rain_adjust 引用保护（流量计不参与雨量补偿，统一防御性排除）。
--   * 删除后若该站流量传感器恢复有效（Q 正常），wtInfo 报文会自动重建新设备并入库，无人工干预。

BEGIN;

-- ---------- 5.1 待删清单落临时表 ----------
CREATE TEMP TABLE _zero_flow ON COMMIT DROP AS
SELECT d.id, d.name, d.type, d.site
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
 WHERE d.name LIKE '%流量计#'
   AND COALESCE(d.type, '') LIKE '%#3#%'
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_adjust ra
                    WHERE ra.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo     t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_river_info t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_info  t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_gate      t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_soil_data t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_nmisp_info t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_pcp_info   t WHERE t.device = d.id);

-- ---------- 5.2 核对清单（预期 11 台：望江分干渠/太湖毕岭/怀宁麻塘湖/望江/汪元管理所/
--               宿松/集岭管理所/双庙湖管理所/毕岭管理所/渠首管理所 + 1 台 8 月历史） ----------
SELECT count(*) AS total FROM _zero_flow;
SELECT z.id, z.name, z.type, d.created_at
  FROM _zero_flow z
  JOIN "qixiao-apaas".t_auto_hltgq_water_device d ON d.id = z.id
 ORDER BY d.created_at DESC;

-- ---------- 5.3 告警/工单断设备引用（内容与状态保留） ----------
UPDATE "qixiao-apaas".t_auto_hltgq_water_alert a
   SET device = NULL, updated_at = now(), updated_by = 'SYSTEM'
  FROM _zero_flow z
 WHERE a.device = z.id;

UPDATE "qixiao-apaas".t_auto_hltgq_water_work_order w
   SET device = NULL, updated_at = now(), updated_by = 'SYSTEM'
  FROM _zero_flow z
 WHERE w.device = z.id;

-- ---------- 5.4 留痕流水断设备引用（防产生新的悬空 device 行） ----------
UPDATE "qixiao-apaas".t_auto_hltgq_water_msg_info m
   SET device = NULL
  FROM _zero_flow z
 WHERE m.device = z.id;

UPDATE "qixiao-apaas".t_auto_hltgq_water_vol_info v
   SET device = NULL
  FROM _zero_flow z
 WHERE v.device = z.id;

-- ---------- 5.5 删除设备行 ----------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_device d
 USING _zero_flow z
 WHERE d.id = z.id;

-- ---------- 5.6 提交前终验：清单内设备应已全部删除（预期 0） ----------
SELECT count(*) AS remaining_devices
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN _zero_flow z ON d.id = z.id;

COMMIT;

-- ==================== 清理后复查（只读） ====================
-- 6.1 零数据流量计# 残留（预期 0；条件同 5.1，用于复查幂等）
SELECT count(*) AS zerodata_flow_left
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
 WHERE d.name LIKE '%流量计#'
   AND COALESCE(d.type, '') LIKE '%#3#%'
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_adjust ra
                    WHERE ra.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo     t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_river_info t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_info  t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_gate      t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_soil_data t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_nmisp_info t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_pcp_info   t WHERE t.device = d.id);

-- 6.2 设备总量与状态（清理后核对）
SELECT COALESCE(status, '(空)') st, count(*) n
  FROM "qixiao-apaas".t_auto_hltgq_water_device
 GROUP BY 1 ORDER BY n DESC;
