-- ============================================================================
-- 清理"天甫斗渠"（stcd 9000000076）遗留数据与孤儿设备
--
-- 背景（2026-11-11 只读核查结论）：
--   1. 旧站点档案已被物理删除：旧 site id = Ouyi0aO4nW6lx0kf0YZ（site 表已无此行）
--   2. 9000000076 已由新站点"瓷岭斗渠"复用：id = wfM8tOXf5JgeQ5PgA2R，11-11 01:31 建档，名下无设备、无数据
--   3. 旧站残留：
--        msg_info 301 行（2026-11-09 15:08 ~ 2026-11-11 09:01，site=旧ID；其中 43 行 device=旧设备）
--        wt_nfo    43 行（同上）
--        vol_info  43 行（同上，device 全空）
--        device     1 行：id=e3hiv5qBtl24Jvroy1UO，name=天甫斗渠流量计#，status=#1#，site 仍指向旧ID（孤儿）
--      其余表（alert/work_order/threshold/gate/gate_action/sluice_discharge/station_sort/
--      inspection_record/moisture_record/rain_adjust/nmisp_info/pcp_info/soil_data/rain_info/
--      river_info）按旧 site/设备核查均为 0 行；stats_daily 无该 stcd 记录
--
-- 关键风险：
--   - 新站复用同一 stcd，禁止按 stcd='9000000076' 删除，否则会误删新站数据
--   - mq 进程内 stcdSiteCache（stcd→site_id）为永久缓存、无 TTL，不重启会把新报文继续写进旧 site id
--
-- 建议执行顺序：停 mq → 执行本脚本 → 启动 mq（新报文自动归新站，并按设备拆分链路自动创建新设备）
-- 执行前如需追溯（可选）：用客户端把命中行导出为文件（csv / insert 脚本），
--   不要把数据以备份表形式留在本库——弃用站点的数据要求"平台不存在"
-- 全库残留核查（2026-11-11 实测，含全部 bak_* 备份表）：
--   含 site 列的表 199 张 → 仅 device(1)/msg_info(301)/vol_info(43)/wt_nfo(43) 有数据
--   含 device 列的表 155 张 → 仅 msg_info(43)/wt_nfo(43) 有数据
--   含 stcd 列的表 48 张 → 仅上述三张业务表有数据（备份表零残留，moisture_detail 为墒情方案表不涉及）
-- ============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1) 删除前预检：应输出 msg_info=301、wt_nfo=43、vol_info=43、device=1、其余=0
-- ---------------------------------------------------------------------------
SELECT 'msg_info' AS tbl, count(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_msg_info   WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'vol_info',  count(*) FROM "qixiao-apaas".t_auto_hltgq_water_vol_info  WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'wt_nfo',    count(*) FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo    WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'river_info',count(*) FROM "qixiao-apaas".t_auto_hltgq_water_river_info WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'rain_info', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_rain_info WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'gate',      count(*) FROM "qixiao-apaas".t_auto_hltgq_water_gate      WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'soil_data', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_soil_data WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'nmisp_info',count(*) FROM "qixiao-apaas".t_auto_hltgq_water_nmisp_info WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'pcp_info',  count(*) FROM "qixiao-apaas".t_auto_hltgq_water_pcp_info  WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'rain_adjust', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_rain_adjust WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'alert',     count(*) FROM "qixiao-apaas".t_auto_hltgq_water_alert      WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'work_order',count(*) FROM "qixiao-apaas".t_auto_hltgq_water_work_order WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'threshold', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_threshold  WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'gate_action',count(*) FROM "qixiao-apaas".t_auto_hltgq_water_gate_action WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'sluice_discharge', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_sluice_discharge WHERE site = 'Ouyi0aO4nW6lx0kf0YZ'
UNION ALL
SELECT 'station_sort', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_station_sort WHERE site = 'Ouyi0aO4nW6lx0kf0YZ'
UNION ALL
SELECT 'inspection_record', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_inspection_record WHERE site = 'Ouyi0aO4nW6lx0kf0YZ'
UNION ALL
SELECT 'moisture_record', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_moisture_record WHERE site = 'Ouyi0aO4nW6lx0kf0YZ'
UNION ALL
SELECT 'device',    count(*) FROM "qixiao-apaas".t_auto_hltgq_water_device WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR id = 'e3hiv5qBtl24Jvroy1UO'
ORDER BY cnt DESC;

-- ---------------------------------------------------------------------------
-- 2) 删除业务数据（只用 site/device 精确条件，禁止用 stcd）
-- ---------------------------------------------------------------------------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_msg_info   WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_vol_info   WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo     WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_river_info WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_rain_info  WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_gate       WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_soil_data  WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_nmisp_info WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_pcp_info   WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_rain_adjust WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';

-- ---------------------------------------------------------------------------
-- 3) 删除关联业务数据（当前核查均为 0 行，执行一次保证干净）
-- ---------------------------------------------------------------------------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_alert      WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_work_order WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_threshold  WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_gate_action WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_sluice_discharge WHERE site = 'Ouyi0aO4nW6lx0kf0YZ';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_station_sort     WHERE site = 'Ouyi0aO4nW6lx0kf0YZ';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_inspection_record WHERE site = 'Ouyi0aO4nW6lx0kf0YZ';
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_moisture_record   WHERE site = 'Ouyi0aO4nW6lx0kf0YZ';

-- ---------------------------------------------------------------------------
-- 4) 统计表（当前 0 行；stats_daily 只有 stcd 无 site，
--    按"新站 11-11 建档"的时间边界只删历史统计，避免误删新站统计）
-- ---------------------------------------------------------------------------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_stats_daily
 WHERE stcd = '9000000076' AND stats_date < DATE '2026-11-11';

-- ---------------------------------------------------------------------------
-- 5) 删除天甫斗渠设备（孤儿设备，site 仍指向已删除的旧站点）
-- ---------------------------------------------------------------------------
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_device
 WHERE id = 'e3hiv5qBtl24Jvroy1UO' OR site = 'Ouyi0aO4nW6lx0kf0YZ';

-- ---------------------------------------------------------------------------
-- 6) 旧站点档案：已于此前被删除，此处仅兜底（预期 0 行）
-- ---------------------------------------------------------------------------
DELETE FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef WHERE id = 'Ouyi0aO4nW6lx0kf0YZ';

-- ---------------------------------------------------------------------------
-- 7) 删除后复核：业务/关联/统计/设备/档案全量核查，应全部为 0；
--    新站点 wfM8tOXf5JgeQ5PgA2R 必须仍存在（new_site = 1）
-- ---------------------------------------------------------------------------
SELECT 'msg_info' AS tbl, count(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_msg_info WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'vol_info', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_vol_info WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'wt_nfo',   count(*) FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo   WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'river_info', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_river_info WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'rain_info', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_rain_info WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'gate',      count(*) FROM "qixiao-apaas".t_auto_hltgq_water_gate      WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'soil_data', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_soil_data WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'nmisp_info', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_nmisp_info WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'pcp_info',  count(*) FROM "qixiao-apaas".t_auto_hltgq_water_pcp_info  WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'rain_adjust', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_rain_adjust WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'alert',     count(*) FROM "qixiao-apaas".t_auto_hltgq_water_alert      WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'work_order', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_work_order WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'threshold', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_threshold  WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'gate_action', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_gate_action WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR device = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'sluice_discharge', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_sluice_discharge WHERE site = 'Ouyi0aO4nW6lx0kf0YZ'
UNION ALL
SELECT 'station_sort', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_station_sort WHERE site = 'Ouyi0aO4nW6lx0kf0YZ'
UNION ALL
SELECT 'inspection_record', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_inspection_record WHERE site = 'Ouyi0aO4nW6lx0kf0YZ'
UNION ALL
SELECT 'moisture_record', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_moisture_record WHERE site = 'Ouyi0aO4nW6lx0kf0YZ'
UNION ALL
SELECT 'stats_daily', count(*) FROM "qixiao-apaas".t_auto_hltgq_water_stats_daily WHERE stcd = '9000000076' AND stats_date < DATE '2026-11-11'
UNION ALL
SELECT 'device',   count(*) FROM "qixiao-apaas".t_auto_hltgq_water_device   WHERE site = 'Ouyi0aO4nW6lx0kf0YZ' OR id = 'e3hiv5qBtl24Jvroy1UO'
UNION ALL
SELECT 'old_site_archive', count(*) FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef WHERE id = 'Ouyi0aO4nW6lx0kf0YZ'
UNION ALL
SELECT 'new_site', count(*) FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef     WHERE id = 'wfM8tOXf5JgeQ5PgA2R';

COMMIT;

-- ============================================================================
-- 执行后事项：
--   1. 重启 hltgq-mq（清掉进程内 stcdSiteCache 旧映射），否则新报文仍会写入旧 site id
--   2. 重启后确认 9000000076 的报文 site 字段 = wfM8tOXf5JgeQ5PgA2R，并自动生成
--      "瓷岭斗渠流量计#"设备（设备拆分链路运行时自动创建）
--   3. Redis 残留（无 DB 权限，需在 mq 侧处理）：
--      - mqtt:gate:queue:{Ouyi0aO4nW6lx0kf0YZ} 队列 24h TTL，最后写入 11-11 09:01，自动过期
--      - mqtt:gate:activeSites 集合中的旧 site id 不会自动清，可执行（幂等，无害）：
--          SREM mqtt:gate:activeSites Ouyi0aO4nW6lx0kf0YZ
--   4. 若需保留"天甫斗渠"历史数据做追溯，请用客户端导出文件，不要落库留备份表
-- ============================================================================

-- ============================================================================
-- 附：全库残留自查 SQL 生成（COMMIT 后单独执行，复制生成的语句再跑）
--   作用：遍历全库所有含 site 列的表（含 bak_* 备份表），确认无任何旧站数据
--   使用：复制生成结果后，去掉最后一行的 "UNION ALL"（或末尾补一行 SELECT 'x',0），即为完整语句
--   注意：KingbaseES MySQL 兼容模式下 || 是逻辑或，拼接一律用 concat()
-- ============================================================================
SELECT concat('SELECT ''', table_name, ''' AS tbl, count(*) AS cnt FROM "qixiao-apaas".',
              table_name, ' WHERE site = ''Ouyi0aO4nW6lx0kf0YZ''', ' UNION ALL') AS chk_sql
FROM information_schema.columns
WHERE table_schema = 'qixiao-apaas' AND column_name = 'site'
ORDER BY table_name;

-- ============================================================================
-- 附：后续弃用站点清理模板（替换 3 个参数即可复用）
--   <OLD_SITE_ID>   弃用站点在 site 表的 id（先查：SELECT id FROM site WHERE iofhpi='<stcd>'）
--   <OLD_DEVICE_IDS> 该站点名下所有设备 id（先查：SELECT id FROM device WHERE site='<OLD_SITE_ID>'）
--   <OLD_STCD>      弃用站点编码（仅 stats_daily 与事后核查使用）
--   清理范围（与本次一致）：9 张业务表 + 9 张关联表 + 统计表 + 设备 + 站点档案；
--   条件只用 site/device，禁止用 stcd（stcd 可能被新站复用，误删新数据）
--   Redis 与 mq 重启按上文"执行后事项"处理
-- ============================================================================
