-- 设备拆分上线：④ 零数据设备清理 + 多值 type 收敛（在部署本批代码并重启之后执行）
-- 生成：2026-10-26；依据：site 联调质疑 A/B2/C 的只读核查结论
--
-- !! 执行顺序 !!
--   1. 必须先构建部署本批代码（MonitorDataService #A 修复 + 告警关闭口径 #B4）并重启应用，
--      否则删掉的设备会被仍运行的旧代码在下一轮报文中重新创建。
--   2. 本脚本第 1 部分（清理）为单事务：执行后先看"核对清单"输出，数量与预期
--      （约 244 台 = 水质监测仪# ~101 + 雨量计# ~96 + 闸孔1# ~47）偏差过大时先 ROLLBACK，
--      不要直接 COMMIT（脚本已内置 COMMIT，核对不满意须在提交前人工中止）。
--   3. B2 多值 type 收敛为独立小事务，最后执行。
--
-- 判定口径（与 site 联调确认一致）：
--   * "有数据" = 7 张业务表（wt_nfo / river_info / rain_info / gate / soil_data / nmisp_info / pcp_info）
--     任一表存在该 device 的行；msg_info / vol_info 留痕流水不计（守卫前统一留痕的旁路记录，
--     不代表有效采集数据——正是它导致这批设备此前"永远在线"）。
--   * 水质监测仪#：排除站点类型含 #8# 的真水质站（防误删水质站设备）；
--   * 雨量计#：排除被 rain_adjust 补偿配置引用的设备；
--   * 闸孔设备：type 含 #4# 且名称以 1# 结尾（lookupOrCreateRtuDevice 闸站固定命名"站名1#"）。
--   * 删除前把 msg_info / vol_info / alert / work_order 中指向待删设备的 device 置 NULL，
--     不产生新的悬空 device 引用（告警/工单内容与状态原样保留，仅断开设备引用）。
--   * 9999900001待接入# 与拆分前历史零数据设备（如 #3#×1、#7#×1）不在本脚本范围，
--     由业主拍板后单独处置。
--   * 历史悬空 device 行（wt_nfo 765 / gate 3789，32 位旧 UUID，7~9 月）不在本脚本范围，
--     如需清理另行出具脚本。

-- ==================== 第 1 部分：零数据设备清理（单事务） ====================
BEGIN;

-- ---------- 1.1 待删清单落临时表（7 张业务表全零 + 命名/类型特征 + 保护条件） ----------
CREATE TEMP TABLE _zero_dev ON COMMIT DROP AS
SELECT d.id, d.name, d.type, d.site
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
 WHERE (
        (d.name LIKE '%水质监测仪#'
             AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
                              WHERE s.id = d.site AND COALESCE(s.epjutj, '') LIKE '%#8#%'))
     OR (d.name LIKE '%雨量计#'
             AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_adjust ra
                              WHERE ra.device = d.id))
     OR (d.name LIKE '%1#' AND COALESCE(d.type, '') LIKE '%#4#%')
       )
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo     t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_river_info t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_info  t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_gate      t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_soil_data t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_nmisp_info t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_pcp_info   t WHERE t.device = d.id);

-- ---------- 1.2 核对清单（人工核对：按 type 汇总 + 全量明细） ----------
SELECT COALESCE(type, '(空)') AS dev_type, count(*) AS cnt
  FROM _zero_dev
 GROUP BY type
 ORDER BY cnt DESC;

SELECT id, name, type
  FROM _zero_dev
 ORDER BY name;

-- ---------- 1.3 告警/工单断设备引用（内容与状态保留） ----------
UPDATE "qixiao-apaas".t_auto_hltgq_water_alert a
   SET device = NULL, updated_at = now(), updated_by = 'SYSTEM'
  FROM _zero_dev z
 WHERE a.device = z.id;

UPDATE "qixiao-apaas".t_auto_hltgq_water_work_order w
   SET device = NULL, updated_at = now(), updated_by = 'SYSTEM'
  FROM _zero_dev z
 WHERE w.device = z.id;

-- ---------- 1.4 留痕流水断设备引用（防产生新的悬空 device 行） ----------
UPDATE "qixiao-apaas".t_auto_hltgq_water_msg_info m
   SET device = NULL
  FROM _zero_dev z
 WHERE m.device = z.id;

UPDATE "qixiao-apaas".t_auto_hltgq_water_vol_info v
   SET device = NULL
  FROM _zero_dev z
 WHERE v.device = z.id;

-- ---------- 1.5 删除设备行 ----------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_device d
 USING _zero_dev z
 WHERE d.id = z.id;

-- ---------- 1.6 提交前终验：清单内设备应已全部删除（预期 0） ----------
SELECT count(*) AS remaining_devices
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN _zero_dev z ON d.id = z.id;

COMMIT;

-- ==================== 第 2 部分：多值 type 收敛（site 联调 #B2） ====================
-- 审计：当前全部多值 type 设备（预期仅 1 台"易潭支渠流量计#"，type='#3#|#5#'）
SELECT id, name, type
  FROM "qixiao-apaas".t_auto_hltgq_water_device
 WHERE type LIKE '%|%';

BEGIN;

-- 流量计设备 type 收敛为单值 #3#（旧代码 deviceType = epjutj 整串写入的历史产物，现行代码不再产生）
UPDATE "qixiao-apaas".t_auto_hltgq_water_device
   SET type = '#3#', updated_at = now(), updated_by = 'SYSTEM'
 WHERE type = '#3#|#5#' AND name LIKE '%流量计#';

COMMIT;

-- ==================== 第 3 部分：清理后复查（只读） ====================
-- 3.1 多值 type 残留（预期 0）
SELECT count(*) AS multi_type_left
  FROM "qixiao-apaas".t_auto_hltgq_water_device
 WHERE type LIKE '%|%';

-- 3.2 本批范围内的零数据设备残留（预期 0；条件同 1.1，用于复查幂等）
SELECT count(*) AS zerodata_left
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
 WHERE (
        (d.name LIKE '%水质监测仪#'
             AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
                              WHERE s.id = d.site AND COALESCE(s.epjutj, '') LIKE '%#8#%'))
     OR (d.name LIKE '%雨量计#'
             AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_adjust ra
                              WHERE ra.device = d.id))
     OR (d.name LIKE '%1#' AND COALESCE(d.type, '') LIKE '%#4#%')
       )
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo     t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_river_info t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_info  t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_gate      t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_soil_data t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_nmisp_info t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_pcp_info   t WHERE t.device = d.id);
