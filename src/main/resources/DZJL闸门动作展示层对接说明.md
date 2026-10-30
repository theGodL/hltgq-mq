# DZJL 闸门动作记录 —— site 展示层对接说明

> 生成日期：2026-10-30
> 数据来源：MQTT `DZJL` 主题（设备方闸门开启/关闭动作报文）
> 写入方：hltgq-mq 服务 `MqttGateActionService`
> 目标读者：站点展示层（site 详情页 / 看板）前端与产品

---

## 1. 背景与数据链路

设备方新增 MQTT 主题 `DZJL`，闸门每次开启/关闭动作时上报，动作期间每秒推送一帧
全量 28 个动作点（3 站 14 孔 × 开启/关闭两点，另含南山寺）。

**关键口径：mq 服务只把"标签变成 1 的时刻"落库为一条动作记录，高频帧在内存中比对后丢弃，
库中不存在每秒数据。每发生一次开/关动作，动作表恰好新增一条记录**，展示层可以按"事件流"直接消费。

```
DZJL 报文（每秒 1 帧，动作期间）
      │
      ▼
MqttGateActionService（内存指纹去重：仅新动作落库）
      │
      ├──▶ t_auto_hltgq_water_gate_action  动作事件表（展示层数据源）
      │
      └──▶ t_auto_hltgq_water_msg_info     原始报文留痕（msg='DZJL'，仅新动作帧，追溯用）
```

**与既有快照链路的分工（互补，互不覆盖）：**

| 表 | 链路口径 | 回答的问题 |
|---|---|---|
| `t_auto_hltgq_water_gate`（gate 表） | HLTData 周期快照 | 「现在开度/水位是多少」（现状） |
| `t_auto_hltgq_water_gate_action`（动作表） | DZJL 动作事件 | 「什么时候开过 / 关过闸」（事件） |

---

## 2. 动作表结构

表：`"qixiao-apaas".t_auto_hltgq_water_gate_action`（15 列，2 个索引）

| 列 | 类型 | 展示层是否使用 | 说明 |
|---|---|---|---|
| `id` | varchar(64) | 否 | 主键 |
| `corp_code` | varchar(64) | 否 | 企业编码（hltgq） |
| `site` | varchar(64) | 关联用 | 站点 id（站点表主键） |
| `site_name` | varchar(200) | **是** | 站点名称，如「南山寺节制闸」 |
| `gate_no` | varchar(16) | **是** | 闸孔号（纯数字文本），如 `"1"` |
| `device` | varchar(64) | 关联用 | 闸孔设备 id（设备表主键，可关联出设备名） |
| `action` | varchar(64) | 追溯用 | 动作标签全名，如 `NSS_ZM1_KQ` |
| `action_type` | varchar(16) | **是** | 动作类型：`open`=开启 / `close`=关闭 |
| `action_time` | timestamp | **是** | 动作发生时间（**设备时标**，见 §3.3） |
| `state` | varchar(200) | 否 | 上报值原始状态（当前恒为 `"1"`） |
| `raw` | text | 否 | 原始报文全文（不要直接展示，仅供追溯） |
| `tm` | timestamp | 排障用 | 报文入库时间（**服务器时间**） |
| `created_at` / `created_by` / `updated_at` / `updated_by` | - | 否 | 审计字段 |

**索引：**
- `idx_water_gate_action_site_tm (site, tm DESC)` —— 页面「某站最近开关记录」主查询路径
- `idx_water_gate_action_tm (tm DESC)` —— 全局最新动作

**数据量预估：** 只记动作不记帧，每天新增为个位数到几十条级别，无清理压力。

---

## 3. 字段语义与展示口径

### 3.1 action_type（动作类型）

| 库值 | 报文标签 | 展示文案 |
|---|---|---|
| `open` | `{前缀}_ZM{孔号}_KQ` | 开启 |
| `close` | `{前缀}_ZM{孔号}_GB` | 关闭 |

建议由前端映射文案（开启/关闭），SQL 里不必转换。

### 3.2 一次操作 = 两条记录，开启时段由配对得出

一次「开→关」完整操作在库中是**两条独立记录**：

```
open   2026-10-30 09:32:28   孔1 开启
close  2026-10-30 09:33:21   孔1 关闭
        └── 开启时段 = close.action_time − open.action_time（本例约 53 秒）
```

- 「开启时间」= 该孔第一条 `open` 记录的 `action_time`
- 「结束时间」= 其后最近一条 `close` 记录的 `action_time`
- 配对规则：同 `site` + 同 `gate_no`，open 之后最近的一个 close（见 §5 查询示例 3）

若产品只需要列表流水，直接按记录逐行展示即可，无需配对。

### 3.3 action_time 是设备时标（重要观察项）

- `action_time` 来自报文里的 `updatetime` 字段，是**设备打的时标**；`tm` 是服务器入库时刻
- 实测设备时钟比服务器**快约 30 秒量级**（历史已知的设备时钟问题）：
  两条真实记录的 action_time 均晚于入库 tm 约 26~32 秒
- 展示建议：列表直接用 `action_time` 展示动作时间（这是动作发生的真实时标）；
  如产品对"动作时间晚于当前时间几十秒"有疑问，口径为设备时钟偏差，不是数据错误
- `tm` 仅用于排障，不建议上界面

### 3.4 site / gate_no / device 三者关系

- `site`：站点 id，可直接关联站点表（`t_auto_hltgq_5nw74_vnqqef.id`）；`site_name` 已冗余存好
- `gate_no`：孔号 1~5；`device`：该孔设备 id，关联设备表 `t_auto_hltgq_water_device.id`
  可取出设备名（口径 = 站名 + 孔号 + `#`，如「南山寺节制闸1#」，type=`#4#`）
- 四站 14 孔全部可对应，实测无孤立记录：

| 报文前缀 | 站点 | 孔数 |
|---|---|---|
| `NSS` | 南山寺节制闸 | 5 |
| `QSDZ` | 渠首电站防洪闸 | 3 |
| `QSJSZ` | 渠首进水闸 | 3 |
| `SMH` | 双庙湖节制闸 | 3 |

### 3.5 与开度/水位（gate 表）联动

动作记录自身不含开度。若要展示「动作 + 当前开度/水位」，按 `site + gate_no`
关联 gate 表最新快照（每站每孔一行，随 HLTData 持续刷新，见 §5 查询示例 4）。
动作前/动作后的开度变化按 `action_time` 前后取快照即可实现，属增强项，非必须。

---

## 4. 展示层最小接入清单

站点详情页新增「最近开关闸记录」列表即可覆盖需求：

1. 数据源：动作表，按站点过滤、按 `action_time DESC` 取最近 N 条（默认 20）
2. 每行展示：**闸孔号 | 动作类型（开启/关闭）| 动作时间 | 设备名（可选）**
3. 展示示例（真实数据）：

   ```
   南山寺节制闸1#   开启    2026-10-30 09:32:28
   南山寺节制闸1#   关闭    2026-10-30 09:33:21
   ```

4. 该孔尚无动作时显示空态（新上线站点属正常，报文只在动作时发送）

**增强项（按需）：**
- 开启时段视图（open/close 配对，展示时长）
- 动作行内联当前开度/上下游水位（关联 gate 快照）
- 按站点统计每日开关次数

---

## 5. 查询示例（已在 KingbaseES 实测通过）

> 注意：本库为 MySQL 兼容模式，**字符串拼接必须用 `concat()`，禁止用 `||`**（`||` 在此库是逻辑或）。

### 示例 1：某站点最近 20 条开关记录（主查询路径，走索引）

```sql
SELECT a.site_name,
       a.gate_no,
       a.action_type,          -- open / close，前端映射显示文案
       a.action_time,
       d.name AS device_name
  FROM "qixiao-apaas".t_auto_hltgq_water_gate_action a
  LEFT JOIN "qixiao-apaas".t_auto_hltgq_water_device d ON d.id = a.device
 WHERE a.site = ?               -- 站点 id 参数
 ORDER BY a.action_time DESC NULLS LAST
 LIMIT 20;
```

### 示例 2：全站最近动作（看板滚动条）

```sql
SELECT a.site_name, a.gate_no, a.action_type, a.action_time
  FROM "qixiao-apaas".t_auto_hltgq_water_gate_action a
 ORDER BY a.tm DESC
 LIMIT 20;
```

### 示例 3：开启时段配对（open → 其后最近 close）

```sql
SELECT o.site_name,
       o.gate_no,
       o.action_time AS open_time,
       c.action_time AS close_time
  FROM "qixiao-apaas".t_auto_hltgq_water_gate_action o
  LEFT JOIN "qixiao-apaas".t_auto_hltgq_water_gate_action c
    ON c.site = o.site
   AND c.gate_no = o.gate_no
   AND c.action_type = 'close'
   AND c.action_time = (
       SELECT min(x.action_time)
         FROM "qixiao-apaas".t_auto_hltgq_water_gate_action x
        WHERE x.site = o.site
          AND x.gate_no = o.gate_no
          AND x.action_type = 'close'
          AND x.action_time > o.action_time)
 WHERE o.site = ?                 -- 站点 id 参数
   AND o.action_type = 'open'
 ORDER BY o.action_time DESC
 LIMIT 50;
```

时长 = `close_time − open_time`，建议前端计算展示（未配对的 open 表示仍在开启中）。

### 示例 4：动作行内联当前开度/水位（关联 gate 最新快照）

```sql
SELECT a.gate_no,
       a.action_type,
       a.action_time,
       g.open_degree,             -- 当前开度（米）
       g.up_z,                    -- 闸上水位
       g.down_z                   -- 闸下水位
  FROM "qixiao-apaas".t_auto_hltgq_water_gate_action a
  LEFT JOIN "qixiao-apaas".t_auto_hltgq_water_gate g
    ON g.site = a.site
   AND g.gate_no = a.gate_no
   AND g.tm = (SELECT max(tm)
                 FROM "qixiao-apaas".t_auto_hltgq_water_gate
                WHERE site = a.site AND gate_no = a.gate_no)
 WHERE a.site = ?
 ORDER BY a.action_time DESC
 LIMIT 20;
```

---

## 6. 注意事项汇总

1. **无高频数据**：设备每秒推帧只是传输载体，库中一次动作仅一行，展示层不会看到"每秒一条"
2. **无重复数据**：同站点+同孔+同标签+同动作时间的记录由入库去重保证唯一
3. **raw 列不上界面**：全文可达数 KB，仅供排障追溯
4. **设备时标偏差**：动作时间来自设备时钟（偏快约 30 秒量级），属已知设备问题，非数据错误
5. **站点字段完整**：历史脏数据（孔号/设备为 NULL 的行）已在 12 号脚本清理回填完毕，
   当前所有记录 site / gate_no / action_type / device 均有值
6. **空态合理**：报文仅在闸门动作时发送，长时间无新记录代表期间无操作，不是链路故障；
   链路健康状态以 gate 表快照持续刷新为准（四站快照约每分钟刷新）
7. **SQL 方言**：KingbaseES MySQL 兼容模式，拼串用 `concat()`；`LIMIT`、窗口函数、`date_trunc` 均可用
