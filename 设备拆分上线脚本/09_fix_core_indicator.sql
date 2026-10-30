-- 档案分类编排：⑨ 修复"纯雨量站"核心指标水位列假值残留（ccnhtm1=27.30）
-- 生成：2026-10-27；依据：核心指标列现状核查（Verify46）发现
--
-- 背景：
--   鲤鱼墩假水位时期（2026-09 之前），水库片区 10 个纯雨量站（档案 #2#）上报的
--   riverInfo Z=27.3 假水位被旧代码同时刷进了站点表核心指标列 ccnhtm1；
--   06_cleanup_fake_waterlevel.sql 清理了 river_info 数据行，但站点表 ccnhtm1
--   的残留值未清（本次核查发现 10 站仍为 27.30）。
--   新代码上线后这些站的 riverInfo 被档案守卫永久拦截（不产心跳），
--   时效巡检对"无心跳 + 有值"的列走保守分支不动 → 27.30 不会自愈，需一次性修复。
--
-- 判定口径（按档案，不硬编码 stcd）：
--   * 水库片区（iofhpi 前缀 32064000）；
--   * 档案不含水位类监测项（epjutj 无 #1# 且无 #4#）；
--   * ccnhtm1 非空且非 '0.00'（幂等，可重复执行）。
--   预期命中 10 行：3206400002/03/04/05/06/08/09/0B/0C/0D（值均为 27.30）。
--   不触碰：周家河/坝上/坝下（#1# 真实水位 93.94/78.06/40.97）、非水库片区站点。
--
-- 说明：置 '0.00' 而非 NULL——与时效巡检/离线归零的既有写法一致（'0.00' 表示无新数据），
--   纯雨量站无水位监测项，该列停留在 '0.00' 属预期形态。

BEGIN;

-- 9.1 核对清单（预期 10 行，全部为 27.30）
SELECT s.iofhpi, s.zzkaec, s.epjutj, s.ccnhtm1
  FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
 WHERE s.iofhpi LIKE '32064000%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#4#%'
   AND s.ccnhtm1 IS NOT NULL
   AND s.ccnhtm1 <> '0.00'
 ORDER BY s.iofhpi;

-- 9.2 修复
UPDATE "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
   SET ccnhtm1 = '0.00', updated_at = now(), updated_by = 'SYSTEM'
 WHERE s.iofhpi LIKE '32064000%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#4#%'
   AND s.ccnhtm1 IS NOT NULL
   AND s.ccnhtm1 <> '0.00';

-- 9.3 提交前终验（预期 0）
SELECT count(*) AS left_fake
  FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
 WHERE s.iofhpi LIKE '32064000%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#4#%'
   AND s.ccnhtm1 IS NOT NULL
   AND s.ccnhtm1 <> '0.00';

COMMIT;

-- ==================== 修复后复查（只读） ====================
-- 10.1 水库 13 站核心指标现状（预期：#1# 水位类 3 站保留真实值，10 个 #2# 站为 '0.00'）
SELECT s.iofhpi, s.zzkaec, s.epjutj, s.ccnhtm1, s.ijzsby1
  FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
 WHERE s.iofhpi LIKE '32064000%' ORDER BY s.iofhpi;

-- 10.2 全库"无水位监测项但 ccnhtm1 有非零值"复查（预期仅闸站 0/null，无假值）
SELECT s.iofhpi, s.zzkaec, s.epjutj, s.ccnhtm1
  FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
 WHERE s.ccnhtm1 IS NOT NULL AND s.ccnhtm1 <> '0.00'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#4#%';
