-- ============================================================================
-- 坝上站(3206400007) 雨量入库补偿 · 历史未修正段一次性修正 SQL
-- ----------------------------------------------------------------------------
-- 背景：站端 9/18 灌数 +34，9/21 14:10 起至补偿上线前入库的行仍带 +34 偏移；
--       mq 入库补偿（DYP-34，写死配置）已上线，本 SQL 将"补偿上线前"的存量行
--       修正到与补偿同口径（序列连续、时段降雨/核心指标口径一致）。
-- 配套文档：src/main/resources/雨量入库补偿方案.md（第三章）
-- 实查依据：2026-09-22 16:24 核查（result32/result33.txt）：
--       未修正段共 88 行，tm 2026-09-21 14:10 ~ 09-22 16:00，dyp 52.4/52.9/53.4；
--       其中 rainfall3h 虚高 19 行、rainfall6h 虚高 24 行（前值落在已修正段所致）。
--
-- 【执行前提 · 必读】
--   1. 先跑第 0 步核对。特别是 0.2：最新入库行 dyp 已是补偿后值(≈19.x)才可执行；
--      若最新行仍是 53.x → 补偿未生效（部署未完成/未重启），先排查部署，否则修正后
--      新报文仍按 +34 入库，会继续产生一批虚高行（届时本 SQL 可重跑，但没必要）。
--   2. 第 1 步必须在第 2 步之前执行（第 2 步重算依赖第 1 步修正后的 dyp 序列）。
--   3. 两步均可安全重复执行：第 1 步防重条件 dyp>40（修正后 ≈18~19 不会再命中）；
--      第 2 步重算幂等（结果与当前 dyp 序列一致时输出不变）。
--   4. 建议单会话逐条执行，先看每步 SELECT 结果，再执行 UPDATE。
--   5. 修正执行后，核心指标(ijzsby1)无需修改 SQL：下一条报文到达时自动按新口径刷新。
-- ============================================================================


-- ============ 第 0 步：修正前核对（只读） ============

-- 0.1 存量未修正段范围（预期 cnt=88，min=52.4，max=53.4；
--     若执行时已过数小时且补偿未生效，新增行也在此列，数目会略增）
SELECT COUNT(*) cnt, MIN(tm) min_tm, MAX(tm) max_tm, MIN(dyp) min_dyp, MAX(dyp) max_dyp
FROM "qixiao-apaas".t_auto_hltgq_water_rain_info
WHERE stcd = '3206400007' AND tm >= '2026-09-21 14:10:00' AND dyp > 40;

-- 0.2 【执行前提】补偿是否已生效：看最新 5 行
--     最新行 dyp≈19.x（如 19.4）→ 已生效，可继续执行第 1/2 步；
--     最新行仍 53.x → 未生效，停止，先排查 mq 部署。
SELECT to_char(tm,'MM-DD HH24:MI') t, dyp, to_char(created_at,'MM-DD HH24:MI:SS') created
FROM "qixiao-apaas".t_auto_hltgq_water_rain_info
WHERE stcd = '3206400007' ORDER BY tm DESC LIMIT 5;

-- 0.3 序列衔接参照（12:00 行=18.4 已修正；14:10 起为 52.4 系列待修正）
SELECT to_char(tm,'MM-DD HH24:MI') t, dyp, rainfall1h, rainfall3h, rainfall6h
FROM "qixiao-apaas".t_auto_hltgq_water_rain_info
WHERE stcd = '3206400007' AND tm >= '2026-09-21 11:00:00' AND tm <= '2026-09-21 17:30:00'
ORDER BY tm;

-- 0.4 修正前快照（存档用，便于回滚核对）：
--     将 0.1 条件(dyp>40)命中行的 tm/dyp/rainfall1h/rainfall3h/rainfall6h 导出留档。


-- ============ 第 1 步：dyp 修正（未修正段统一 -34） ============

UPDATE "qixiao-apaas".t_auto_hltgq_water_rain_info
SET dyp = dyp - 34,
    updated_at = now(),
    updated_by = 'DATA_FIX'
WHERE stcd = '3206400007'
  AND tm >= '2026-09-21 14:10:00'
  AND tm <  '2026-09-23 00:00:00'   -- 一次性边界：只修部署生效前的存量；如实际执行日在 9/23 之后且期间有旧库漏网行，按实调整
  AND dyp > 40;                      -- 防重：修正后 ≈18~19，重复执行不会二次扣减
-- 预期：UPDATE 88（+ 部署生效前可能新增的未修正行数）

-- 1.1 即时复核：14:10 行应变为 18.4，与 12:00 行(18.4)连续；全段不再有 >40 的行
SELECT to_char(tm,'MM-DD HH24:MI') t, dyp, (dyp - LAG(dyp) OVER (ORDER BY tm)) diff
FROM "qixiao-apaas".t_auto_hltgq_water_rain_info
WHERE stcd = '3206400007' AND tm >= '2026-09-21 12:00:00'
ORDER BY tm;
-- 预期：34 断差消失；9/21 23:15 与 9/22 13:24 处 diff=0.5（真实降雨），其余 0


-- ============ 第 2 步：时段降雨重算（rainfall1h/3h/6h） ============
-- 按修正后的 dyp 序列、与 mq 完全一致的窗口/守卫重算（queryPreviousDyp/computeRainfall 口径）：
--   r1h 前值窗口 [tm-2h, tm-1h]、r3h [tm-6h, tm-3h]、r6h [tm-12h, tm-6h]，取窗口内最近一行；
--   前值需 dyp>0 且非 4294967295 哨兵；差值∈[0, 上限] 才写（上限 500/1500/3000）；
--   窗口内无有效前值 → 保持原值（原本 NULL 则保持 NULL，与 mq "不写入"行为一致）。
-- 范围 = 9/21 14:10 起全部行（不设上界）：
--   既修正虚高的存量行，也顺带补"补偿生效后~本 SQL 执行前"因负跳变而缺列的少量新行
--   （若存在）。执行后新到报文由 mq 实时计算（前值已同口径），无需再处理。

-- 2.0 预览对照（只读，推荐先跑）：old=当前值 new=重算值
--     预期差异仅出现在：r3h/r6h 虚高 ±34 的行；其余 old=new（或 new 为 NULL 时保留 old）
SELECT to_char(r.tm,'MM-DD HH24:MI') t,
       r.rainfall1h AS old_r1h, r.dyp - (SELECT p.dyp FROM "qixiao-apaas".t_auto_hltgq_water_rain_info p
             WHERE p.stcd=r.stcd AND p.device=r.device AND p.dyp IS NOT NULL AND p.dyp > 0 AND p.dyp <> 4294967295
               AND p.tm >= r.tm - interval '2 hours'  AND p.tm <= r.tm - interval '1 hour'
             ORDER BY p.tm DESC LIMIT 1) AS new_r1h,
       r.rainfall3h AS old_r3h, r.dyp - (SELECT p.dyp FROM "qixiao-apaas".t_auto_hltgq_water_rain_info p
             WHERE p.stcd=r.stcd AND p.device=r.device AND p.dyp IS NOT NULL AND p.dyp > 0 AND p.dyp <> 4294967295
               AND p.tm >= r.tm - interval '6 hours'  AND p.tm <= r.tm - interval '3 hours'
             ORDER BY p.tm DESC LIMIT 1) AS new_r3h,
       r.rainfall6h AS old_r6h, r.dyp - (SELECT p.dyp FROM "qixiao-apaas".t_auto_hltgq_water_rain_info p
             WHERE p.stcd=r.stcd AND p.device=r.device AND p.dyp IS NOT NULL AND p.dyp > 0 AND p.dyp <> 4294967295
               AND p.tm >= r.tm - interval '12 hours' AND p.tm <= r.tm - interval '6 hours'
             ORDER BY p.tm DESC LIMIT 1) AS new_r6h
FROM "qixiao-apaas".t_auto_hltgq_water_rain_info r
WHERE r.stcd = '3206400007' AND r.tm >= '2026-09-21 14:10:00'
ORDER BY r.tm;

-- 2.1 执行重算（幂等，可重复执行）
UPDATE "qixiao-apaas".t_auto_hltgq_water_rain_info r
SET rainfall1h = CASE WHEN s.d1 IS NOT NULL AND s.d1 >= 0 AND s.d1 <= 500  THEN s.d1 ELSE r.rainfall1h END,
    rainfall3h = CASE WHEN s.d3 IS NOT NULL AND s.d3 >= 0 AND s.d3 <= 1500 THEN s.d3 ELSE r.rainfall3h END,
    rainfall6h = CASE WHEN s.d6 IS NOT NULL AND s.d6 >= 0 AND s.d6 <= 3000 THEN s.d6 ELSE r.rainfall6h END,
    updated_at = now(),
    updated_by = 'DATA_FIX'
FROM (
    SELECT r2.id,
        r2.dyp - (SELECT p.dyp FROM "qixiao-apaas".t_auto_hltgq_water_rain_info p
              WHERE p.stcd=r2.stcd AND p.device=r2.device AND p.dyp IS NOT NULL AND p.dyp > 0 AND p.dyp <> 4294967295
                AND p.tm >= r2.tm - interval '2 hours'  AND p.tm <= r2.tm - interval '1 hour'
              ORDER BY p.tm DESC LIMIT 1) AS d1,
        r2.dyp - (SELECT p.dyp FROM "qixiao-apaas".t_auto_hltgq_water_rain_info p
              WHERE p.stcd=r2.stcd AND p.device=r2.device AND p.dyp IS NOT NULL AND p.dyp > 0 AND p.dyp <> 4294967295
                AND p.tm >= r2.tm - interval '6 hours'  AND p.tm <= r2.tm - interval '3 hours'
              ORDER BY p.tm DESC LIMIT 1) AS d3,
        r2.dyp - (SELECT p.dyp FROM "qixiao-apaas".t_auto_hltgq_water_rain_info p
              WHERE p.stcd=r2.stcd AND p.device=r2.device AND p.dyp IS NOT NULL AND p.dyp > 0 AND p.dyp <> 4294967295
                AND p.tm >= r2.tm - interval '12 hours' AND p.tm <= r2.tm - interval '6 hours'
              ORDER BY p.tm DESC LIMIT 1) AS d6
    FROM "qixiao-apaas".t_auto_hltgq_water_rain_info r2
    WHERE r2.stcd = '3206400007' AND r2.tm >= '2026-09-21 14:10:00'
) s
WHERE r.id = s.id;


-- ============ 第 3 步：修正后总验收（只读） ============

-- 3.1 全段序列总览（预期：18.4 连续 → 23:15 起 18.9 → 9/22 13:24 起 19.4，无断差）
SELECT to_char(tm,'MM-DD HH24:MI') t, dyp, rainfall1h, rainfall3h, rainfall6h,
       (dyp - LAG(dyp) OVER (ORDER BY tm)) diff
FROM "qixiao-apaas".t_auto_hltgq_water_rain_info
WHERE stcd = '3206400007' AND tm >= '2026-09-21 11:00:00'
ORDER BY tm;

-- 3.2 残留检查（预期 0 行）
SELECT COUNT(*) AS remain_bad_rows
FROM "qixiao-apaas".t_auto_hltgq_water_rain_info
WHERE stcd = '3206400007' AND tm >= '2026-09-21 14:10:00' AND dyp > 40;

-- 3.3 时段降雨抽查（预期：9/21 23:15 行 r1h=0.5；9/22 13:24 行 r1h=0.5；无 34/34.5 虚高残留）
SELECT to_char(tm,'MM-DD HH24:MI') t, dyp, rainfall1h, rainfall3h, rainfall6h
FROM "qixiao-apaas".t_auto_hltgq_water_rain_info
WHERE stcd = '3206400007'
  AND (tm IN ('2026-09-21 23:15:00','2026-09-22 13:24:00','2026-09-21 14:10:00')
       OR tm >= '2026-09-21 15:00:00' AND tm <= '2026-09-21 16:30:00'
       OR tm >= '2026-09-21 19:35:00' AND tm <= '2026-09-21 20:15:00')
ORDER BY tm;

-- 3.4 核心指标观察（无需 SQL 修正）：等待下一条 rainInfo 报文入库后，
--     站点核心指标 ijzsby1 会在该报文处理时按修正后口径自动刷新；
--     若次日仍无更新，检查 mq 最新报文是否正常入库（mq 处理链路健康性）。
