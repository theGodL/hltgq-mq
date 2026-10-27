-- 档案分类编排：⑦ 清理"水库片区纯水位站"的雨量计#设备与雨量数据（花凉亭坝下）
-- 生成：2026-10-27；依据：用户拍板"花凉亭坝下是纯水位站，雨量计# 不应该存在"
--
-- 背景：
--   花凉亭坝下（320640000A）档案 epjutj=#1# 纯水位站，无雨量监测项；
--   站端 RTU 自 2026-08-05 起持续上报 rainInfo 报文（DYP 2.5~88mm 有真实波动），
--   10-26 设备拆分时按数据归属建出"花凉亭坝下雨量计#"（type=#2#）并入库 rain_info 3961 行。
--   按 2026-10-27 晚定版口径：水库13站以档案编排，纯水位站的雨量报文视为错误数据——
--   mq 侧已新增"雨量报文站点档案守卫"（willBeGuardedOut + 主链路，仅留痕不入库、
--   不建雨量计#设备），与水位方向守卫同一模式；本脚本清理存量设备与数据。
--
-- !! 执行顺序 !!
--   1. 必须先部署 2026-10-27 晚版档案守卫代码并重启，否则删掉的设备会被站端下一轮
--      rainInfo 报文重新创建（坝下雨量报文仍在持续投递，15:20 仍有入库行）；
--   2. 单事务执行，先看"核对清单"输出（预期 1 台 / 3961 行 rain_info / 1 条 alert），
--      偏差过大时人工中止后再 COMMIT；
--   3. 断链口径与 04/05/06 一致：告警内容与状态保留，仅断开设备引用；
--      msg_info 报文留痕保留（反映站端真实投递事实），仅置 device=NULL。
--
-- 判定口径（按档案，不硬编码设备 id）：
--   * 设备名称以"雨量计#"结尾且 type 含 #2#；
--   * 所属站点属于水库片区（iofhpi 前缀 32064000）且档案 epjutj 不含 #2#；
--   * 防御性排除：无 rain_adjust 引用；除 rain_info 以外 6 张业务表全零
--     （wt_nfo/gate/soil_data/nmisp_info/pcp_info/river_info，防误删带真实数据的设备）。

BEGIN;

-- ---------- 7.1 待删清单落临时表 ----------
CREATE TEMP TABLE _baxxia_rain ON COMMIT DROP AS
SELECT d.id, d.name, d.type, d.site, s.zzkaec, s.iofhpi, s.epjutj
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s ON s.id = d.site
 WHERE d.name LIKE '%雨量计#'
   AND COALESCE(d.type, '') LIKE '%#2#%'
   AND s.iofhpi LIKE '32064000%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#2#%'
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_adjust ra
                    WHERE ra.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo      t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_gate        t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_soil_data   t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_nmisp_info  t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_pcp_info    t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_river_info  t WHERE t.device = d.id);

-- ---------- 7.2 核对清单（预期 1 台：花凉亭坝下 雨量计#，2026-10-26 15:33:49 创建，
--               rain_info 共 3961 行，所属站点档案 #1#） ----------
SELECT count(*) AS total_devices FROM _baxxia_rain;
SELECT w.zzkaec, w.iofhpi, w.epjutj, w.name, w.id,
       (SELECT count(*) FROM "qixiao-apaas".t_auto_hltgq_water_rain_info r
         WHERE r.device = w.id) rain_rows
  FROM _baxxia_rain w
 ORDER BY w.iofhpi;

-- ---------- 7.3 告警/工单断设备引用（内容与状态保留；预期告警 1 条、工单 0） ----------
UPDATE "qixiao-apaas".t_auto_hltgq_water_alert a
   SET device = NULL, updated_at = now(), updated_by = 'SYSTEM'
  FROM _baxxia_rain w
 WHERE a.device = w.id;

UPDATE "qixiao-apaas".t_auto_hltgq_water_work_order o
   SET device = NULL, updated_at = now(), updated_by = 'SYSTEM'
  FROM _baxxia_rain w
 WHERE o.device = w.id;

-- ---------- 7.4 留痕流水断设备引用（报文留痕保留，防悬空 device；预期 msg_info 70 行、
--               vol_info 0 行——挂水位计# 的拆分前历史留痕不动） ----------
UPDATE "qixiao-apaas".t_auto_hltgq_water_msg_info m
   SET device = NULL
  FROM _baxxia_rain w
 WHERE m.device = w.id;

UPDATE "qixiao-apaas".t_auto_hltgq_water_vol_info v
   SET device = NULL
  FROM _baxxia_rain w
 WHERE v.device = w.id;

-- ---------- 7.5 删除坝下雨量业务数据（按 stcd 删除该站全部 rain_info，预期 3961 行） ----------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_rain_info r
 WHERE r.stcd = '320640000A';

-- ---------- 7.6 删除设备行 ----------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_device d
 USING _baxxia_rain w
 WHERE d.id = w.id;

-- ---------- 7.7 提交前终验 ----------
-- 清单内设备应已全部删除（预期 0）
SELECT count(*) AS remaining_devices
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN _baxxia_rain w ON d.id = w.id;

-- 坝下雨量数据残留（预期 0）
SELECT count(*) AS remaining_rain
  FROM "qixiao-apaas".t_auto_hltgq_water_rain_info r
 WHERE r.stcd = '320640000A';

COMMIT;

-- ==================== 清理后复查（只读） ====================
-- 8.1 水库片区档案不含 #2# 的雨量计#设备残留（预期 0；条件同 7.1，幂等复查）
SELECT count(*) AS baxxia_rain_left
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s ON s.id = d.site
 WHERE d.name LIKE '%雨量计#'
   AND COALESCE(d.type, '') LIKE '%#2#%'
   AND s.iofhpi LIKE '32064000%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#2#%';

-- 8.2 坝下 rain_info 残留（预期 0）
SELECT count(*) AS baxxia_rain_rows
  FROM "qixiao-apaas".t_auto_hltgq_water_rain_info r
 WHERE r.stcd = '320640000A';

-- 8.3 坝下设备现状（预期只剩 花凉亭坝下水位计# 1 台）
SELECT d.name, d.type, d.created_at
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s ON s.id = d.site
 WHERE s.iofhpi = '320640000A' ORDER BY d.created_at;

-- 8.4 设备总量核对（清理前 449 台，预期 448 台）
SELECT count(*) AS device_total FROM "qixiao-apaas".t_auto_hltgq_water_device;

-- 8.5 水库 13 站雨量计# 设备核查（档案含 #2# 的 12 站各应有 1 台，坝下应无）
SELECT s.iofhpi, s.zzkaec, s.epjutj,
       (SELECT count(*) FROM "qixiao-apaas".t_auto_hltgq_water_device d
         WHERE d.site = s.id AND d.name LIKE '%雨量计#') rain_dev_cnt
  FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
 WHERE s.iofhpi LIKE '32064000%'
 ORDER BY s.iofhpi;
