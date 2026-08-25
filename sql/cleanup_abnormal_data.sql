-- =====================================================================
-- 存量异常数据筛查与修复脚本（数值守卫审计配套，2026-08-14）
-- 目标库: KingbaseES v8 (PostgreSQL 协议)
--        jdbc:postgresql://10.68.18.5:54321/qixiao-apaas?currentSchema=qixiao-apaas
--
-- 背景: 应用层新增数值物理守卫后，新入库数据已合规；本脚本用于清理
--       守卫上线前已落库的历史异常值（如 FFFFFFFF=4294967295 通讯异常
--       哨兵值、负水位、负开度、超限值等），避免展示层出现离奇数据。
--
-- 原则:
--   1. 真实设备数据行不可删除（历史数据不可丢失），仅将异常字段置 NULL，
--      NULL 即"无数据"，展示层按缺失处理；
--   2. gate_no='0' 占位行为改造前脏数据（非真实设备数据），允许删除，
--      应用已内置每小时自动清理任务，此处提供存量一次性清理；
--   3. 守卫边界与应用代码常量一致：
--      水位 (0,1000]m；开度 [0,50]m；电压 (0,100]V；流量 [0,100]m³/s；
--      日雨量 [0,5000]mm；DI 仅 0/1；4294967295 一律视为通讯异常。
--
-- 执行建议: 先执行【第一部分 筛查】确认数量符合预期，再执行
--           【第二部分 修复】；建议在维护窗口执行，执行前备份相关表。
-- =====================================================================

-- ==================== 第一部分：筛查（只读） ====================

-- 1. river_info 通用水位异常（z / z1 / z2）
SELECT 'river_info.z异常'  AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_river_info
WHERE z  IS NOT NULL AND (z  <= 0 OR z  > 1000 OR z  = 4294967295);
SELECT 'river_info.z1异常' AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_river_info
WHERE z1 IS NOT NULL AND (z1 <= 0 OR z1 > 1000 OR z1 = 4294967295);
SELECT 'river_info.z2异常' AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_river_info
WHERE z2 IS NOT NULL AND (z2 <= 0 OR z2 > 1000 OR z2 = 4294967295);

-- 2. gate 表上游/下游水位异常
SELECT 'gate.up_z异常'   AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_gate
WHERE up_z   IS NOT NULL AND (up_z   <= 0 OR up_z   > 1000 OR up_z   = 4294967295);
SELECT 'gate.down_z异常' AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_gate
WHERE down_z IS NOT NULL AND (down_z <= 0 OR down_z > 1000 OR down_z = 4294967295);

-- 3. gate 表开度异常（负值 / >50m）
SELECT 'gate.open_degree异常' AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_gate
WHERE open_degree IS NOT NULL AND (open_degree < 0 OR open_degree > 50);

-- 4. vol_info 电压异常
SELECT 'vol_info.vol异常' AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_vol_info
WHERE vol IS NOT NULL AND (vol <= 0 OR vol > 100 OR vol = 4294967295);

-- 5. wt_nfo 瞬时流量/累计流量异常
SELECT 'wt_nfo.q异常'  AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo
WHERE q  IS NOT NULL AND (q  < 0 OR q  > 100 OR q  = 4294967295);
SELECT 'wt_nfo.tf异常' AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_wt_nfo
WHERE tf IS NOT NULL AND tf = 4294967295;

-- 6. rain_info 累计雨量/日雨量异常
SELECT 'rain_info.dyp异常' AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_rain_info
WHERE dyp IS NOT NULL AND (dyp <= 0 OR dyp = 4294967295);
SELECT 'rain_info.drp异常' AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_rain_info
WHERE drp IS NOT NULL AND (drp < 0 OR drp > 5000 OR drp = 4294967295);

-- 7. gate 表 DI 状态异常（非0/1）
SELECT 'gate.DI状态异常' AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_gate
WHERE (local      IS NOT NULL AND local      NOT IN (0,1))
   OR (remote     IS NOT NULL AND remote     NOT IN (0,1))
   OR (open_run   IS NOT NULL AND open_run   NOT IN (0,1))
   OR (close_run  IS NOT NULL AND close_run  NOT IN (0,1))
   OR (full_open  IS NOT NULL AND full_open  NOT IN (0,1))
   OR (full_close IS NOT NULL AND full_close NOT IN (0,1))
   OR (overload   IS NOT NULL AND overload   NOT IN (0,1))
   OR (power_ok   IS NOT NULL AND power_ok   NOT IN (0,1));

-- 8. gate_no='0' 占位行存量（改造前脏数据）
SELECT 'gate.gate_no=0占位行' AS item, COUNT(*) AS cnt FROM "qixiao-apaas".t_auto_hltgq_water_gate
WHERE gate_no = '0';

-- ==================== 第二部分：修复（字段NULL化，不删行） ====================
-- 每类修复前请先看第一部分对应筛查数量；批量执行前建议 BEGIN ... COMMIT 包裹

-- 1. river_info 水位异常 → NULL
UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
SET z = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX'
WHERE z IS NOT NULL AND (z <= 0 OR z > 1000 OR z = 4294967295);

UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
SET z1 = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX'
WHERE z1 IS NOT NULL AND (z1 <= 0 OR z1 > 1000 OR z1 = 4294967295);

UPDATE "qixiao-apaas".t_auto_hltgq_water_river_info
SET z2 = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX'
WHERE z2 IS NOT NULL AND (z2 <= 0 OR z2 > 1000 OR z2 = 4294967295);

-- 2. gate 表水位异常 → NULL
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate
SET up_z = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX'
WHERE up_z IS NOT NULL AND (up_z <= 0 OR up_z > 1000 OR up_z = 4294967295);

UPDATE "qixiao-apaas".t_auto_hltgq_water_gate
SET down_z = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX'
WHERE down_z IS NOT NULL AND (down_z <= 0 OR down_z > 1000 OR down_z = 4294967295);

-- 3. gate 表开度异常 → NULL
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate
SET open_degree = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX'
WHERE open_degree IS NOT NULL AND (open_degree < 0 OR open_degree > 50);

-- 4. vol_info 电压异常 → NULL
UPDATE "qixiao-apaas".t_auto_hltgq_water_vol_info
SET vol = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX'
WHERE vol IS NOT NULL AND (vol <= 0 OR vol > 100 OR vol = 4294967295);

-- 5. wt_nfo 流量异常 → NULL
UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
SET q = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX'
WHERE q IS NOT NULL AND (q < 0 OR q > 100 OR q = 4294967295);

UPDATE "qixiao-apaas".t_auto_hltgq_water_wt_nfo
SET tf = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX'
WHERE tf IS NOT NULL AND tf = 4294967295;

-- 6. rain_info 雨量异常 → NULL
UPDATE "qixiao-apaas".t_auto_hltgq_water_rain_info
SET dyp = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX'
WHERE dyp IS NOT NULL AND (dyp <= 0 OR dyp = 4294967295);

UPDATE "qixiao-apaas".t_auto_hltgq_water_rain_info
SET drp = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX'
WHERE drp IS NOT NULL AND (drp < 0 OR drp > 5000 OR drp = 4294967295);

-- 7. gate 表 DI 状态异常 → NULL（逐字段，与筛查第7项对应）
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate SET local      = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX' WHERE local      IS NOT NULL AND local      NOT IN (0,1);
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate SET remote     = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX' WHERE remote     IS NOT NULL AND remote     NOT IN (0,1);
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate SET open_run   = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX' WHERE open_run   IS NOT NULL AND open_run   NOT IN (0,1);
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate SET close_run  = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX' WHERE close_run  IS NOT NULL AND close_run  NOT IN (0,1);
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate SET full_open  = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX' WHERE full_open  IS NOT NULL AND full_open  NOT IN (0,1);
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate SET full_close = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX' WHERE full_close IS NOT NULL AND full_close NOT IN (0,1);
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate SET overload   = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX' WHERE overload   IS NOT NULL AND overload   NOT IN (0,1);
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate SET power_ok   = NULL, updated_at = now(), updated_by = 'SYSTEM-DATA-FIX' WHERE power_ok   IS NOT NULL AND power_ok   NOT IN (0,1);

-- 8. gate_no='0' 占位行：改造前脏数据，允许删除
--    （应用每小时定时任务也在自动清理，此处为存量一次性清理）
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_gate WHERE gate_no = '0';

-- =====================================================================
-- 附注: 水位缓存行 open_degree 为 NULL 属既定设计（10分钟补全窗口内
--       无开度报文时的归档结果），不是异常数据，本脚本不处理。
-- =====================================================================
