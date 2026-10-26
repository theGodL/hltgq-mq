# mq 侧复核清单（2026-10-26 二次复核）

> 复核方：hltgq-site（只读通道 `172.27.177.30:54321/qixiao-apaas`）
> 复核时点：2026-10-26 17:25
> 结论摘要：**上一轮 7 项已修 5 项，站点-设备-数据三层已自洽；hltgq-site 无需适配改动。**
> 本文档只列 mq 侧还需确认或核查的事项，全部为非阻塞项。

---

## 一、业务口径（业主 2026-10-26 确认，以下三类不再作为问题跟踪）

1. **完全没有设备的站点** = 预先创建站点，设备尚未接入 → 正常
2. **有设备但监测表无数据** = 设备未接入 → 正常
3. **档案 `epjutj` 为空的站** = 工程节点，仅作父节点分类，不挂设备 → 正常

---

## 二、需 mq 验证的 3 项

### 2.1 30 个"声明了监测类型但站内无任何设备"的站 → 已验证从未接入

| 站点 | stcd | 档案类型 | 报文总条数 |
|---|---|---|---|
| 矮农边涵 | 9000000131 | #1# | 0 |
| 北干渠进口交水点 | 3408279007 | #3# | 0 |
| 北门坦 | 3408279005 | #3# | 0 |
| 南干渠进口交水点 | 3408279010 | #3# | 0 |
| 四号渡槽 | 3408269007 | #3# | 0 |
| 塔岭隧洞 | 3408259004 | #3# | 0 |
| 天甫斗渠 | 9000000076 | #3# | 0 |
| 太宿干渠县界交水点 | 3408269006 | #3# | 0 |
| 太怀干渠长喉槽 | 3408229002 | #3# | 0 |
| 幸福直灌涵 | **NULL** | #3# | 0 |
| 李桥支渠 | 3408279003 | #3# | 0 |
| 杨岭支渠 | **NULL** | #3# | 0 |
| 沙改灌溉涵 | **NULL** | #3# | 0 |
| 焦老屋斗渠 | 9000000071 | #3# | 0 |
| 甘庄支渠 | **NULL** | #3# | 0 |
| 碎石岭渡槽 | 3408279004 | #3# | 0 |
| 程岭许岭交界点 | 3408269001 | #3# | 0 |
| 茂林支渠 | **NULL** | #3# | 0 |
| 褒隐寺水库 | 3408279002 | #3# | 0 |
| 长岭支渠 | **NULL** | #3# | 0 |
| 陈良支渠 | **NULL** | #3# | 0 |
| 雷埠支渠 | 3408279009 | #3# | 0 |
| 高岭长铺交界点 | 3408269002 | #3# | 0 |
| 鸦滩长岭交水点 | 3408279001 | #3# | 0 |
| 下仓分干进水闸 | 3408269003 | #3#\|#4# | 0 |
| 姑塘进水闸 | 3408269004 | #3#\|#4# | 0 |
| 沈家大屋闸 | 3408259005 | #3#\|#4# | 0 |
| 金铁岭节制闸 | 3408279008 | #3#\|#4# | 0 |
| 高岭分干渠进水闸 | 3408259003 | #3#\|#4# | 0 |
| zj_测试 | 334455 | #5# | 0 |

**site 侧已查明**：以上 30 个站在 `msg_info` 的报文条数**全部为 0**，即从未收到过任何报文，不是设备被清理逻辑误删。

**请 mq 确认两点**：
1. `insertMsgLog`（MonitorDataService 第 488 行）位于所有业务守卫之前，是否覆盖**全部 tag**（含被守卫拦截的报文与设备解析失败的报文）？若存在某类报文不留痕的分支，请说明——否则"报文 0 条"就不能作为"从未接入"的判据。
2. 其中 7 个站 `iofhpi` 为 NULL（幸福直灌涵、杨岭支渠、沙改灌溉涵、甘庄支渠、茂林支渠、长岭支渠、陈良支渠）。这些站连 STCD 都没有，按第 342-345 行的逻辑报文会被直接丢弃，永远不会自动建设备。请确认这是预期行为。

### 2.2 有设备但监测表零数据

- **全量口径**：非视频设备 235 台中，只有 3 台零数据——`导入测试-测试设备1`（#3#）、`导入测试-测试设备2`（#7#）、`流量站点-1-9999900001待接入#`（type 空）。全是测试站。
- **近 24h 口径**：32 台无新数据（有历史数据，属停报），其中 21 台 status 已被 `checkOfflineDevices` 正确标为 `#2#`。

**请 mq 确认**：这些设备建账时必然收到过报文（否则不会被创建），现在 0 行说明报文被守卫全部拦截或设备停报。请在日志中抽检 1-2 台，确认是 `wtInfo 瞬时流量异常(Q=...)` / `rainInfo 无有效雨量数据` 这类 WARN，而不是入库异常（`报文处理失败, 丢弃消息`）。

### 2.3 站点档案声明的类型多于站内设备

| 缺失类型 | 站数 | 说明 |
|---|---|---|
| 缺闸门 `#4#` | 74 | gate 表**全部 0 行**，即这些站从未有过闸门数据 |
| 缺流量 `#3#` | 39 | 其中 35 个 wt_nfo 也 0 行；4 个为 4 大闸站历史流量（见 3.3） |
| 缺水位 `#1#` | 23 | river_info 全部 0 行 |
| 缺水质 `#8#` | 3 | — |
| 缺雨量 `#2#` | 1 | — |

按口径 2 属正常（档案预声明，设备未接入）。**仅需 mq 确认一个行为**：这些站将来若真有报文到达，设备是否会被 `lookupOrCreateDeviceByName` 自动补建（幂等，不会重复建）？确认后即可关闭此项。

---

## 三、历史遗留数据（请 mq 核查来源，不影响展示）

### 3.1 wt_nfo 4 个孤儿 device（765 行）

| device | 行数 | stcd | 最早 | 最新 |
|---|---|---|---|---|
| 5uymkF3Xkry0GAgks75x | 627 | 9999900001（测试站） | **2000-01-01 00:00:00** | 2026-08-28 09:30 |
| bpgIUOHeeMUapcICgfnI | 116 | 9999900001（测试站） | 2026-08-07 10:00 | 2026-08-13 16:00 |
| 3LV3k1XpDtUItmNpIM0b | 20 | 9000000006 | 2026-08-03 08:00 | 2026-08-04 12:00 |
| m8SNgkppNbaRjFkUq7Sa | 2 | 9999900001（测试站） | 2026-09-10 10:15 | 2026-09-10 10:30 |

**请核查**：
1. 这 4 个 id 的格式与平台 `IdGenerator` 一致（20 位混合大小写），是否是 mq 旧版本建过、后来被 `cleanupOrphanDevice` 删除设备但数据未迁移？
2. 第一条 `2000-01-01 00:00:00` 是明显的脏时间（tm 解析失败兜底？），请确认当前 `extractTm` 是否还会产生该值。
3. 现在还会不会新增此类孤儿？（近 2h wt_nfo 的 device 全部能关联到设备，说明已不再产生）

### 3.2 gate 11 个孤儿 device（3789 行）

全部集中在 **2026-07-20 ~ 07-22** 三天，每台设备只对应 1 个闸孔：

| device | 行数 |
|---|---|
| 89cc70d3d1444fb89b109562c8c3a70a | 696 |
| dc370e10678e4d959be7b2171e12c657 | 696 |
| ce6548f70a6b46aaa8f3d256ad2830f0 | 696 |
| 59e5de54ba704f9cb3bb864a2292047c | 487 |
| 390f1bf0c8d447a4b39af0dfa2cbad54 | 487 |
| 0c9aa3b1f4ad483db59a78760c462cac | 487 |
| 8619800b9c8f4f64939d0ffc38ac0cff | 48 |
| 2f578ff061ed461eb54b4004c7d3cbf9 | 48 |
| c0240ddbff07495eaf4b2fe613e5a8c7 | 48 |
| 21bf9719ea8f481eabcc6851aa785ece | 48 |
| 1cf394416462497b9f347f68a97a1371 | 48 |

**请核查**：这批 id 是 **32 位无横线 UUID**，与平台 `IdGenerator` 格式不同，是否 7 月旧版本代码或临时脚本写入？当前 `insertGateFromGatesInfo` 写的是 `lookupOrCreateDeviceByName` 返回的设备 id，请确认不会再次产生。

### 3.3 4 大闸站 8 台 `#4#` 设备挂 19524 行历史流量

| 站点 | device | 设备名 | 类型 | stcd | 行数 | 最新 |
|---|---|---|---|---|---|---|
| 渠首进水闸 | CAYQ739MiBWMg9gQvy3 | 渠首进水闸1# | #4# | NULL | 5645 | 2026-10-21 14:42 |
| 双庙湖节制闸 | mqG9dLawRteYWHgjqF3 | 双庙湖节制闸1# | #4# | NULL | 4924 | 2026-10-10 02:02 |
| 渠首电站防洪闸 | 2RR3HvjVi1ONzFewDi3 | 渠首电站防洪闸1# | #4# | NULL | 4597 | 2026-10-21 09:31 |
| 南山寺节制闸 | dByBR1Xdze8shnRGAw5 | 南山寺节制闸1# | #4# | NULL | 4337 | 2026-10-05 23:49 |
| 南山寺节制闸 | dByBR1Xdze8shnRGAw4 | 南山寺节制闸2# | #4# | NULL | 17 | 2026-09-30 16:36 |
| 渠首进水闸 | CAYQ739MiBWMg9gQvy2 | 渠首进水闸2# | #4# | NULL | 1 | 2026-09-22 00:52 |
| 双庙湖节制闸 | mqG9dLawRteYWHgjqF2 | 双庙湖节制闸2# | #4# | NULL | 1 | 2026-10-03 06:08 |
| 南山寺节制闸 | dByBR1Xdze8shnRGAw1 | 南山寺节制闸5# | #4# | NULL | 1 | 2026-09-20 08:21 |

**请核查**：这 19524 行流量的来源。若是 `GateDischargeCalculator` 按闸门开度推算后写入 `wt_nfo`，则 device 挂闸孔设备是合理的（推算源就是闸孔），属于正常；若是老传感器报文，则需要建独立 `#3#` 设备并迁移。另请确认上线后是否还会新增（当前近 2h `wt_nfo` 全部挂 `#3#`，看起来已停）。

---

## 四、虎形潭渡槽：在线但业务断流 6 天

| 设备 | type | status | 最后业务数据 | 近 24h msg_info | 近 24h vol_info |
|---|---|---|---|---|---|
| 虎形潭渡槽水位计# | #1# | **#1#** | river_info 2026-10-20 08:19 | 9 条 | 0 |
| 虎形潭渡槽流量计# | #3# | **#1#** | wt_nfo 2026-10-20 08:19 | **955 条** | 0 |

**请核查**：
1. 这 955 条 `msg_info` 的 `msg` 字段是什么（`msgInfo` 心跳，还是其他 tag 的流水）？`msg_text` 原始报文里有没有业务字段？
2. `riverInfo` / `wtInfo` 报文是否还在投递？若在投递但被守卫拦截，日志里应有 `riverInfo 水位Z无效` / `wtInfo 瞬时流量异常` 的 WARN，请确认具体是哪一条。
3. 判断结论是"RTU 在线、探头损坏/未接"（通信正常、数据无效），还是"业务报文完全没来"（只有心跳）。
4. 设备在线口径确认：当前 `checkOfflineDevices` 把 `msg_info` / `vol_info` 计入数据源，所以只要有心跳报文就判在线。业主要求**维持该口径**（通信在线 ≠ 有数据），此处只需 mq 确认判断依据符合预期，不需要改代码。

---

## 五、其他观察（供参考，不需要处理）

1. **`nmisp_info` 水质数据停在 2026-10-11**（7250 行）。唯一的水质设备"南干进水闸水质站水质监测仪#"status 已被 `checkOfflineDevices` 标为 `#2#`，判定正确。数据源断流需业主联系水质设备方。
2. **3 台测试设备零数据**（导入测试 2 台、流量站点-1 1 台），status 均已标 `#2#`。
3. **22 个无设备站**：12 个空档案工程节点（望江县管理局、宿松县管理所、太湖县管理所、市直管理、怀宁县管理所、骨干工程、放空隧洞、滚水坝等）+ 9 个 `#6#` 模型站 + 1 个测试站 `zj_测试`，符合口径 3。

---

## 六、site 侧复核结论（2026-10-26 二次复核，供对照）

以下为已验证通过项，mq 无需处理：

- 设备台账 456 台：视频 221 / 流量 124 / 闸门 67 / 雨量 20 / 水位 16 / 墒情 6 / 水质 1 / 空 1
- 非视频 235 台中 232 台有数据（上一轮 197 台零数据已清理完毕）
- 近 2h `river_info`/`rain_info`/`wt_nfo`/`gate` 四表：device 零空、类型单一（#1#/#2#/#3#/#4# 各归各类）
- 设备名重复 0、设备 site 空/无效 0、type 多值 0
- 告警 670 行中指向已删设备 **0 行**；未关闭 55 条 device 空 0、设备不存在 0
- 雨量补偿配置指向 `花凉亭坝上雨量计#`（#2#），设备存在
- 未关闭告警设备类型分布：#5# 32 / #3# 19 / #4# 2 / #8# 1
- 闸门监测页按 `gate` 表取数、与设备表解耦；站点详情按 `site` 查设备表；告警/工单 `LEFT JOIN` 设备表无悬空
- **hltgq-site 不需要适配改动**

---

## 七、参考 SQL（只读，mq 自查用）

```sql
-- 30 个无设备站的报文痕迹（结果应全为 0）
SELECT s.zzkaec, s.iofhpi, s.epjutj,
       (SELECT count(*) FROM "qixiao-apaas".t_auto_hltgq_water_msg_info m WHERE m.stcd = s.iofhpi) AS 报文条数
FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s
WHERE NOT EXISTS (SELECT 1 FROM "qixiao-apaas"."t_auto_hltgq_water_device" d WHERE d.site = s.id)
  AND s.epjutj IS NOT NULL AND s.epjutj <> '' AND s.epjutj NOT LIKE '%#6#%'
ORDER BY 4 DESC;

-- 孤儿 device（四表通用，换表名即可）
SELECT m.device, count(*) AS 行数, min(m.tm), max(m.tm)
FROM "qixiao-apaas"."t_auto_hltgq_water_wt_nfo" m
WHERE m.device IS NOT NULL AND m.device <> ''
  AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas"."t_auto_hltgq_water_device" d WHERE d.id = m.device)
GROUP BY 1 ORDER BY 2 DESC;

-- 站点档案声明 vs 站内设备缺口
SELECT
  count(*) FILTER (WHERE s.epjutj LIKE '%#1#%' AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas"."t_auto_hltgq_water_device" d WHERE d.site=s.id AND d.type LIKE '%#1#%')) AS 缺水位,
  count(*) FILTER (WHERE s.epjutj LIKE '%#2#%' AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas"."t_auto_hltgq_water_device" d WHERE d.site=s.id AND d.type LIKE '%#2#%')) AS 缺雨量,
  count(*) FILTER (WHERE s.epjutj LIKE '%#3#%' AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas"."t_auto_hltgq_water_device" d WHERE d.site=s.id AND d.type LIKE '%#3#%')) AS 缺流量,
  count(*) FILTER (WHERE s.epjutj LIKE '%#4#%' AND NOT EXISTS (SELECT 1 FROM "qixiao-apaas"."t_auto_hltgq_water_device" d WHERE d.site=s.id AND d.type LIKE '%#4#%')) AS 缺闸门
FROM "qixiao-apaas".t_auto_hltgq_5nw74_vnqqef s WHERE s.epjutj IS NOT NULL AND s.epjutj <> '';

-- 设备 status 与近 24h 业务数据是否一致
WITH act AS (
  SELECT device FROM "qixiao-apaas".t_auto_hltgq_water_river_info WHERE tm >= now() - interval '24 hour' AND device <> ''
  UNION SELECT device FROM "qixiao-apaas".t_auto_hltgq_water_rain_info WHERE tm >= now() - interval '24 hour' AND device <> ''
  UNION SELECT device FROM "qixiao-apaas"."t_auto_hltgq_water_wt_nfo" WHERE tm >= now() - interval '24 hour' AND device <> ''
  UNION SELECT device FROM "qixiao-apaas"."t_auto_hltgq_water_gate" WHERE tm >= now() - interval '24 hour' AND device <> ''
)
SELECT d.status, count(*) AS 台数,
       count(*) FILTER (WHERE a.device IS NOT NULL) AS 有数据,
       count(*) FILTER (WHERE a.device IS NULL) AS 无数据
FROM "qixiao-apaas"."t_auto_hltgq_water_device" d
LEFT JOIN act a ON a.device = d.id
WHERE COALESCE(d.type,'') <> '#5#'
GROUP BY 1 ORDER BY 2 DESC;
```
