-- 档案分类编排：⑧ 清理"档案外墒情设备"及其全 0 试发数据（新坝支渠进水闸）
-- 生成：2026-10-27；依据：用户拍板"确保数据库里都是有效数据"+ 全库形态扫描结论
--
-- 背景：
--   新坝支渠进水闸（9000000047）档案 epjutj=#3#|#5#|#4#，不含墒情监测项 #7#；
--   站端 RTU 于 2026-09-13 试发 soilData 报文 14 行后停止（16:20 一分钟内密集 8 条，
--   三层墒情 mten/mtwenty/mthirty 全为 0.0/-0.0），之后 6 周再未上报；
--   10-26 设备拆分上线时按历史数据归属回建"新坝支渠进水闸墒情探测#"（type=#7#，
--   创建时间 2026-10-26 15:33:49），soil_data 14 行全部挂它。
--   形态判定与「花凉亭坝下雨量」同型：恒定值（全 0）+ 设备未真实接入（试发报文）。
--
-- !! 执行顺序 !!
--   1. 本脚本不依赖 07（坝下雨量清理）的先后，可独立执行，单事务；
--   2. 执行前看"核对清单"输出（预期 1 台设备 / 14 行 soil_data），偏差则人工中止；
--   3. 新坝为非水库站，不触发任何档案守卫——若站端将来恢复发送 soilData 报文，
--      设备与数据会重新创建；9-13 后已静默 6 周，恢复概率极低，暂不为其加专用守卫
--      （待业务确认全 0 墒情报文是否需按"数值无效"拦截，届时另行拍板）。
--
-- 判定口径（按档案+名称，不硬编码设备 id）：
--   * 设备名称以"墒情探测#"结尾且 type 含 #7#；
--   * 所属站点档案 epjutj 不含 #7#（档案外设备）；
--   * 防御性排除：msg_info/vol_info/alert/work_order/rain_adjust 及
--     wt_nfo/gate/river_info/rain_info/nmisp_info/pcp_info 全零引用（01-27 核查实测全 0）。
--
-- 引用面核查结论（2026-10-27 Verify40）：全部零引用，无需断链 UPDATE；
--   同站保留：新坝支渠进水闸_1（#5# 闸门）、新坝支渠进水闸流量计#（#3#）。

BEGIN;

-- ---------- 8.1 待删清单落临时表 ----------
CREATE TEMP TABLE _xinba_soil ON COMMIT DROP AS
SELECT d.id, d.name, d.type, d.site, s.zzkaec, s.iofhpi, s.epjutj, d.created_at
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s ON s.id = d.site
 WHERE d.name LIKE '%墒情探测#'
   AND COALESCE(d.type, '') LIKE '%#7#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#7#%'
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_msg_info   t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_vol_info   t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_alert      t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_work_order t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_adjust t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo     t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_gate       t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_river_info t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_rain_info  t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_nmisp_info t WHERE t.device = d.id)
   AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas".t_auto_hltgq_water_pcp_info   t WHERE t.device = d.id);

-- ---------- 8.2 核对清单（预期 1 台：新坝支渠进水闸墒情探测#，2026-10-26 15:33:49 创建，
--              soil_data 共 14 行，所属站点档案 #3#|#5#|#4#） ----------
SELECT count(*) AS total_devices FROM _xinba_soil;
SELECT w.zzkaec, w.iofhpi, w.epjutj, w.name, w.id,
       (SELECT count(*) FROM "qixiao-apaas".t_auto_hltgq_water_soil_data x
         WHERE x.device = w.id) soil_rows
  FROM _xinba_soil w
 ORDER BY w.iofhpi;

-- ---------- 8.3 删除全 0 试发墒情数据（按 device，预期 14 行） ----------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_soil_data x
 USING _xinba_soil w
 WHERE x.device = w.id;

-- ---------- 8.4 删除设备行（预期 1 行） ----------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_device d
 USING _xinba_soil w
 WHERE d.id = w.id;

-- ---------- 8.5 提交前终验 ----------
-- 清单内设备应已全部删除（预期 0）
SELECT count(*) AS remaining_devices
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN _xinba_soil w ON d.id = w.id;

-- 新坝 soil_data 残留（预期 0）
SELECT count(*) AS remaining_soil
  FROM "qixiao-apaas".t_auto_hltgq_water_soil_data x
 WHERE x.stcd = '9000000047';

COMMIT;

-- ==================== 清理后复查（只读） ====================
-- 9.1 档案外墒情设备残留（预期 0；条件同 8.1 前四行，幂等复查）
SELECT count(*) AS xinba_soil_left
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s ON s.id = d.site
 WHERE d.name LIKE '%墒情探测#'
   AND COALESCE(d.type, '') LIKE '%#7#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#7#%';

-- 9.2 新坝设备现状（预期只剩 _1（#5# 闸门）与 流量计#（#3#）2 台）
SELECT d.name, d.type, d.created_at
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
  JOIN "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s ON s.id = d.site
 WHERE s.iofhpi = '9000000047' ORDER BY d.created_at;

-- 9.3 4 个真墒情站（#7#）设备与数据完好性核查（预期 4 站各 1 台墒情探测#、行数与清理前一致，
--     2026-10-27 实测：太湖毕岭 2997 / 望江 3009 / 宿松 3087 / 怀宁麻塘湖 3003，数据仍在增长）
SELECT s.iofhpi, s.zzkaec,
       (SELECT count(*) FROM "qixiao-apaas".t_auto_hltgq_water_device d
         WHERE d.site = s.id AND d.name LIKE '%墒情探测#') soil_dev_cnt,
       (SELECT count(*) FROM "qixiao-apaas".t_auto_hltgq_water_soil_data x
         WHERE x.stcd = s.iofhpi) soil_rows
  FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
 WHERE s.epjutj LIKE '%#7#%'
 ORDER BY s.iofhpi;

-- 9.4 设备总量核对（07+08 都执行后：449 - 坝下 1 - 新坝 1 = 预期 447 台）
SELECT count(*) AS device_total FROM "qixiao-apaas".t_auto_hltgq_water_device;
