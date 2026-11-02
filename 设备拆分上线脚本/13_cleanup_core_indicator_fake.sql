-- 核心指标历史假数据清理（double 遗留列 ccnhtm + 实时列 ccnhtm1）
-- 生成：2026-11-02；依据：hltgq-site 反馈「10 个纯雨量站 ccnhtm 恒为 27.3」
--
-- 背景（2026-11-02 只读核查结论）：
--   1) ccnhtm/ijzsby（double precision）是 2026-09-11 之前旧版 mq 写入的历史快照列；
--      2026-09-11（核心指标 v5）起 mq 只写 ccnhtm1/ijzsby1（varchar 文本列），
--      double 列从此无人维护 —— 水位站 79.08/42.48 也是当时的旧值，不再更新。
--   2) 10 个纯雨量站的 ccnhtm=27.3 是"鲤鱼墩假水位时期"（2026-09 之前）旧版代码
--      把站端恒定假水位 Z=27.3 刷进该列的残留，不是今天的写入。
--   3) 同一批假值也残留在 ccnhtm1=27.30（09_fix_core_indicator.sql 口径），
--      本脚本 13.3 与 09.2 等价，二选一执行即可（幂等）。
--
-- 判定口径（按档案，不硬编码 stcd/站名）：
--   * 水库片区（iofhpi 前缀 32064000）；
--   * 档案不含水位类监测项（epjutj 无 #1# 且无 #4#）；
--   * 幂等，可重复执行。
--
-- 不触碰：坝上 3206400007 / 坝下 320640000A / 周家河 3206400001 / 虎形潭 9000000127
--   的真实历史水位（它们有水位监测项，值虽陈旧但非假数据）。
--
-- 注意：执行后这些行的 updated_at 会变为执行时刻（行级时间戳），
--   与"每天有报文到达刷新 updated_at"不是一回事，勿据此判断数据新鲜度。

BEGIN;

-- 13.1 核对清单（预期 10 行，ccnhtm 全部为 27.3、ccnhtm1 全部为 27.30）
SELECT s.iofhpi, s.zzkaec, s.epjutj, s.ccnhtm, s.ccnhtm1
  FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
 WHERE s.iofhpi LIKE '32064000%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#4#%'
   AND (s.ccnhtm IS NOT NULL
        OR (s.ccnhtm1 IS NOT NULL AND s.ccnhtm1 <> '0.00'))
 ORDER BY s.iofhpi;

-- 13.2 清理 double 遗留列：无水位监测项的站置 NULL
--   （该列已停止维护，置 NULL 表示"该站无水位监测项"，符合 site 期望口径）
UPDATE "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
   SET ccnhtm = NULL, updated_at = now(), updated_by = 'SYSTEM'
 WHERE s.iofhpi LIKE '32064000%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#4#%'
   AND s.ccnhtm IS NOT NULL;

-- 13.3 清理实时列残留（与 09_fix_core_indicator.sql 9.2 等价，已执行过则 0 行）
UPDATE "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
   SET ccnhtm1 = '0.00', updated_at = now(), updated_by = 'SYSTEM'
 WHERE s.iofhpi LIKE '32064000%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#4#%'
   AND s.ccnhtm1 IS NOT NULL
   AND s.ccnhtm1 <> '0.00';

-- 13.4 提交前终验（预期 0）
SELECT count(*) AS left_fake
  FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
 WHERE s.iofhpi LIKE '32064000%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#1#%'
   AND COALESCE(s.epjutj, '') NOT LIKE '%#4#%'
   AND (s.ccnhtm IS NOT NULL
        OR (s.ccnhtm1 IS NOT NULL AND s.ccnhtm1 <> '0.00'));

COMMIT;

-- ==================== 修复后复查（只读） ====================
-- 14.1 水库 13 站核心指标现状
--   预期：#1# 站 ccnhtm 保留旧水位、ccnhtm1 为实时值；#2# 站 ccnhtm 为 NULL、ccnhtm1='0.00'
SELECT s.iofhpi, s.zzkaec, s.epjutj, s.ccnhtm, s.ijzsby, s.ccnhtm1, s.ijzsby1
  FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
 WHERE s.iofhpi LIKE '32064000%'
 ORDER BY s.iofhpi;

-- 14.2 全库 double 水位列非零（预期只剩有水位监测项的真水位站）
SELECT s.zzkaec, s.iofhpi, s.epjutj, s.ccnhtm
  FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
 WHERE s.ccnhtm IS NOT NULL AND s.ccnhtm <> 0
 ORDER BY s.ccnhtm DESC;
