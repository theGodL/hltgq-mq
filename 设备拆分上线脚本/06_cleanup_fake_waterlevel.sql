-- 档案分类编排：⑥ 清理"档案非水位类站点"的水位计#设备与假水位数据
-- 生成：2026-10-27；依据：任务单"完全以站点档案（epjutj 监测类型）分类进行逻辑编排"定版
--
-- 背景：
--   水库片区 32064000xx 水文遥测网 13 站中，10 个雨量站（epjutj=#2#，不含 #1#/#4#）
--   被站端 RTU 持续上报 riverInfo 水位报文，Z 恒定为配置值 27.3（假水位，非实测）。
--   10-26 设备拆分时按数据归属自动建出 10 台"XX水位计#"设备并入库 river_info 6507 行。
--   按 2026-10-27 定版口径：档案非水位类站点的水位报文视为错误数据——
--   mq 侧已新增"水位报文站点档案守卫"（willBeGuardedOut + 主链路，仅留痕不入库、
--   不建水位计#设备）；本脚本清理存量设备与假数据。
--
-- !! 执行顺序 !!
--   1. 必须先部署 2026-10-27 档案守卫代码并重启，否则删掉的设备会被站端下一轮
--      riverInfo 报文重新创建（与 05 脚本同一前置条件）；
--   2. 单事务执行，先看"核对清单"输出（预期 10 台 / 6507 行 river_info），
--      偏差过大时人工中止后再 COMMIT；
--   3. 断链口径与 04/05 一致：告警/工单内容与状态保留，仅断开设备引用；
--      msg_info 报文留痕保留（反映站端真实投递事实），仅置 device=NULL。
--
-- 判定口径（按档案，不硬编码设备 id/站名）：
--   * 设备名称以"水位计#"结尾且 type 含 #1#；
--   * 所属站点档案 epjutj 不含 #1# 且不含 #4#（#4# 闸站闸前/闸后水位属合法监测项）；
--   * 防御性排除：无 rain_adjust 引用；除 river_info 以外 7 张业务表全零
--     （防误删未来其它口径下带有真实数据的设备）。

BEGIN;

-- ---------- 6.1 待删清单落临时表 ----------
CREATE TEMP TABLE _fake_wlevel ON COMMIT DROP AS
SELECT d.id, d.name, d.type, d.site, s.zzkaec, s.iofhpi, s.epjutj
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s ON s.id = d.site
 WHERE d.name LIKE '%水位计#'
   AND COALESCE(d.type, '') LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#4#%'
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_adjust ra
                    WHERE ra.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo      t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_info   t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_gate        t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_soil_data   t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_nmisp_info  t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_pcp_info    t WHERE t.device = d.id);

-- ---------- 6.2 核对清单（预期 10 台：姜家寨/九田/下前河/牛镇/马嘶铺/寺前/河图铺/
--               鲤鱼墩/弥陀/白帽，全部 2026-10-26 15:33:49 创建，river_info 共 6507 行） ----------
SELECT count(*) AS total_devices FROM _fake_wlevel;
SELECT w.zzkaec, w.iofhpi, w.epjutj, w.name, w.id,
       (SELECT count(*) FROM "qixiao-apaas".t_auto_hltgq_water_river_info r
         WHERE r.device = w.id) riv_rows
  FROM _fake_wlevel w
 ORDER BY w.iofhpi;

-- ---------- 6.3 告警/工单断设备引用（内容与状态保留；核查期已知本批为 0） ----------
UPDATE "qixiao-apaas".t_auto_hltgq_water_alert a
   SET device = NULL, updated_at = now(), updated_by = 'SYSTEM'
  FROM _fake_wlevel w
 WHERE a.device = w.id;

UPDATE "qixiao-apaas".t_auto_hltgq_water_work_order o
   SET device = NULL, updated_at = now(), updated_by = 'SYSTEM'
  FROM _fake_wlevel w
 WHERE o.device = w.id;

-- ---------- 6.4 留痕流水断设备引用（报文留痕保留，防悬空 device） ----------
UPDATE "qixiao-apaas".t_auto_hltgq_water_msg_info m
   SET device = NULL
  FROM _fake_wlevel w
 WHERE m.device = w.id;

UPDATE "qixiao-apaas".t_auto_hltgq_water_vol_info v
   SET device = NULL
  FROM _fake_wlevel w
 WHERE v.device = w.id;

-- ---------- 6.5 删除假水位业务数据（river_info，预期 6507 行） ----------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_river_info r
 USING _fake_wlevel w
 WHERE r.device = w.id;

-- ---------- 6.6 删除设备行 ----------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_device d
 USING _fake_wlevel w
 WHERE d.id = w.id;

-- ---------- 6.7 提交前终验 ----------
-- 清单内设备应已全部删除（预期 0）
SELECT count(*) AS remaining_devices
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN _fake_wlevel w ON d.id = w.id;

-- 假数据残留（预期 0）
SELECT count(*) AS remaining_riv
  FROM "qixiao-apaas".t_auto_hltgq_water_river_info r
  JOIN _fake_wlevel w ON r.device = w.id;

COMMIT;

-- ==================== 清理后复查（只读） ====================
-- 7.1 档案非水位类站点仍残留的水位计#设备（预期 0；条件同 6.1，幂等复查）
SELECT count(*) AS fake_wlevel_left
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s ON s.id = d.site
 WHERE d.name LIKE '%水位计#'
   AND COALESCE(d.type, '') LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#4#%';

-- 7.2 10 站 river_info 残留（预期 0；若有值需人工核对是否为 device=NULL 悬挂行）
SELECT r.stcd, count(*) n
  FROM "qixiao-apaas".t_auto_hltgq_water_river_info r
 WHERE r.stcd IN ('3206400002','3206400003','3206400004','3206400005','3206400006',
                  '3206400008','3206400009','320640000B','320640000C','320640000D')
 GROUP BY r.stcd;

-- 7.3 水库片区 13 站水位数据现状（预期只剩 3 个真水位站有 river_info）
SELECT r.stcd, count(*) n, min(r.z) min_z, max(r.z) max_z
  FROM "qixiao-apaas".t_auto_hltgq_water_river_info r
 WHERE r.stcd IN ('3206400001','3206400002','3206400003','3206400004','3206400005','3206400006',
                  '3206400007','3206400008','3206400009','320640000A','320640000B','320640000C','320640000D')
 GROUP BY r.stcd ORDER BY r.stcd;

-- 7.4 设备总量核对（清理前 459 台，预期 449 台）
SELECT count(*) AS device_total FROM "qixiao-apaas".t_auto_hltgq_water_device;
