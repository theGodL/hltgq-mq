-- ============================================================
-- 11 闸门动作记录表扩列：device（对应闸孔设备）、action_type（开/关语义）
-- 依据：设备方数据点组态表 HLT20260915（{前缀}_ZM{孔号}_{KQ|GB}）
-- 执行时机：部署新版代码【之前】执行（服务启动时按信息模式加载列元数据）
-- 执行人：DBA / SYSTEM
-- ============================================================

-- 1. 扩列：device=闸孔设备 id（对齐 water_device #4# 设备）；action_type=open(开启)/close(关闭)
ALTER TABLE "qixiao-apaas".t_auto_hltgq_water_gate_action
    ADD COLUMN IF NOT EXISTS device varchar(64);

ALTER TABLE "qixiao-apaas".t_auto_hltgq_water_gate_action
    ADD COLUMN IF NOT EXISTS action_type varchar(16);

COMMENT ON COLUMN "qixiao-apaas".t_auto_hltgq_water_gate_action.device IS '闸孔设备id(t_auto_hltgq_water_device.type=#4#，名称口径=站名+孔号#)';
COMMENT ON COLUMN "qixiao-apaas".t_auto_hltgq_water_gate_action.action_type IS '动作类型：open=开启(KQ) / close=关闭(GB)';

-- 2. 复查：应见 device、action_type 两列
SELECT column_name, data_type, character_maximum_length
  FROM information_schema.columns
 WHERE table_schema = 'qixiao-apaas'
   AND table_name = 't_auto_hltgq_water_gate_action'
 ORDER BY ordinal_position;
