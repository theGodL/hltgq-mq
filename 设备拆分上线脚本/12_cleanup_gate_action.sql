-- ============================================================
-- 12 闸门动作记录：历史行回填（闸孔/动作类型/设备）+ 清理重复行
-- 背景：部署修复前的去重 SQL 因 null 参数类型推断失败被降级放行，
--       设备持续帧导致同一动作（同 action_time）被反复入库（如 NSS_ZM1_GB 40+ 条）。
-- 执行时机：执行 11 扩列脚本并部署新版代码【之后】执行（先止血再清理，避免边清边涨）。
-- 执行人：DBA / SYSTEM
-- 说明：动作本身真实发生，只删重复帧，每个动作保留 1 条（tm 最早的一条）。
-- ============================================================

-- 1. 回填闸孔号 / 动作类型（标签名格式 {前缀}_ZM{孔号}_{KQ|GB}）
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate_action
   SET gate_no     = substring(action from '_ZM([0-9]+)_'),
       action_type = CASE WHEN action ~ '_KQ$' THEN 'open'
                          WHEN action ~ '_GB$' THEN 'close' END,
       updated_at  = now(),
       updated_by  = 'SYSTEM'
 WHERE action ~ '_ZM[0-9]+_(KQ|GB)$'
   AND (gate_no IS NULL OR action_type IS NULL);

-- 2. 回填 device：站名+孔号# 匹配闸孔设备（type=#4#，如 南山寺节制闸1#）
-- 注意：本库为 KingbaseES MySQL 兼容模式，|| 是逻辑或而非字符串连接，拼串必须用 concat()
UPDATE "qixiao-apaas".t_auto_hltgq_water_gate_action a
   SET device     = d.id,
       updated_at = now(),
       updated_by = 'SYSTEM'
  FROM "qixiao-apaas".t_auto_hltgq_water_device d
 WHERE a.device IS NULL
   AND a.site IS NOT NULL
   AND a.gate_no IS NOT NULL
   AND a.site_name IS NOT NULL
   AND d.site = a.site
   AND d.name = concat(a.site_name, a.gate_no, '#')
   AND d.type LIKE '%#4#%';

-- 3.1 预览重复组（务必先看，确认清理范围）
SELECT site_name, gate_no, action, action_time, count(*) AS cnt,
       min(tm) AS first_tm, max(tm) AS last_tm
  FROM "qixiao-apaas".t_auto_hltgq_water_gate_action
 GROUP BY site_name, gate_no, action, action_time
HAVING count(*) > 1
 ORDER BY cnt DESC;

-- 3.2 清理：同 site+gate_no+action+action_time 每组保留 tm 最早一条
DELETE FROM "qixiao-apaas".t_auto_hltgq_water_gate_action
 WHERE id IN (
       SELECT id FROM (
              SELECT id,
                     row_number() OVER (
                         PARTITION BY COALESCE(site, site_name, ''), COALESCE(gate_no, ''),
                                      COALESCE(action, ''), action_time
                         ORDER BY tm, id) AS rn
                FROM "qixiao-apaas".t_auto_hltgq_water_gate_action
       ) t
        WHERE rn > 1);

-- 4. 复查：剩余动作记录（预期每个真实动作 1 条，device 均已挂接）
SELECT a.site_name, a.gate_no, a.action, a.action_type, a.action_time, a.state,
       (SELECT d.name FROM "qixiao-apaas".t_auto_hltgq_water_device d WHERE d.id = a.device) AS device_name
  FROM "qixiao-apaas".t_auto_hltgq_water_gate_action a
 ORDER BY a.action_time, a.action;
