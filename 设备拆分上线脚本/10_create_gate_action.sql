-- 档案分类编排：⑩ 新建"闸门动作记录"表（DZJL topic 落库载体）
-- 生成：2026-10-30
--
-- 背景：
--   设备方新增 MQTT topic DZJL，用于监控闸门开启/关闭动作（动作时间 + 执行状态）。
--   现有 HLTData 快照链路（gate 表）只有"当前状态"，无"动作发生时间"；
--   本表逐条记录开/关动作事件，与 gate 表互补（一快照一事件，互不覆盖）。
--
-- 写入方：mq 服务 MqttGateActionService（DZJL 报文到达即落库，含原始报文 raw 全量保留）。
-- 字段说明：
--   site/site_name  报文解析出的站点（前缀 NSS 等映射站名后查 zzkaec；匹配不到为 NULL）
--   gate_no         闸孔号（纯数字文本，解析不到为 NULL）
--   action          动作类型原始值（v1 原样保留，字段映射待真实样例校准）
--   action_time     动作发生时间（报文携带时间解析值；解析不到为 NULL，raw 可追溯）
--   state           执行状态原始值
--   raw             DZJL 原始报文全文（信息兜底，任何解析失败都可在 raw 追查）
--   tm              报文接收时间（服务器时间）
-- 去重：mq 侧按 site+gate_no+action+action_time 查重，Broker 重投/重启补投不产生重复行。

BEGIN;

-- 10.1 建表（幂等）
CREATE TABLE IF NOT EXISTS "qixiao-apaas".t_auto_hltgq_water_gate_action (
    id           varchar(64)  NOT NULL,
    corp_code    varchar(64),
    site         varchar(64),
    site_name    varchar(200),
    gate_no      varchar(16),
    action       varchar(64),
    action_time  timestamp,
    state        varchar(200),
    raw          text,
    tm           timestamp,
    created_at   timestamp,
    created_by   varchar(64),
    updated_at   timestamp,
    updated_by   varchar(64),
    CONSTRAINT pk_water_gate_action PRIMARY KEY (id)
);

-- 10.2 索引：按站点查最近动作（页面"最近开关记录"主查询路径）
CREATE INDEX IF NOT EXISTS idx_water_gate_action_site_tm
    ON "qixiao-apaas".t_auto_hltgq_water_gate_action (site, tm DESC);

CREATE INDEX IF NOT EXISTS idx_water_gate_action_tm
    ON "qixiao-apaas".t_auto_hltgq_water_gate_action (tm DESC);

-- 10.3 终验（预期 1 行，列表头）
SELECT count(*) AS table_ready
  FROM information_schema.columns
 WHERE table_schema = 'qixiao-apaas'
   AND table_name = 't_auto_hltgq_water_gate_action';

COMMIT;

-- ==================== 部署后复查（只读） ====================
-- 11.1 DZJL 原始报文留痕（确认报文到达与结构）
SELECT tm, site, msg, LEFT(msg_text, 300) AS head
  FROM "qixiao-apaas".t_auto_hltgq_water_msg_info
 WHERE msg = 'DZJL' ORDER BY tm DESC LIMIT 20;

-- 11.2 动作记录明细（确认解析效果）
SELECT tm, site_name, gate_no, action, action_time, state
  FROM "qixiao-apaas".t_auto_hltgq_water_gate_action
 ORDER BY tm DESC LIMIT 20;
