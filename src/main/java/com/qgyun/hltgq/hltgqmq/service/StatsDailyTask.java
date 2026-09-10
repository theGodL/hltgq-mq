package com.qgyun.hltgq.hltgqmq.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 日汇总任务：每日 00:05 生成昨日统计快照写入 t_auto_hltgq_water_stats_daily，
 * 区间查询读汇总表聚合，查询复杂度与天数解耦（业主查任意长区间均毫秒级）。
 * <p>
 * 行编码约定（复用 stcd 列区分行类型，主键 (stats_date, stcd) 无需变更）：
 * <ul>
 *   <li>站点行：stcd=站点id，arrival_* 存到报口径、collect_* 存站点级采集口径(缺测窗=应测-成功)</li>
 *   <li>维度行：stcd="__collect_"+维度键(river/rain/wt/gate/soil)，collect_* 存维度采集口径</li>
 *   <li>全局行：stcd="__global__"，msg_rows/stored_rows 存当日报文/入库行数</li>
 * </ul>
 * 写入幂等：先删当日再插入（单日行数少，事务内完成）。快照口径与实时接口共用
 * {@link ArrivalStatsService#computeDaySnapshot(LocalDate)}，无双口径风险。
 */
@Component
@EnableScheduling
public class StatsDailyTask {

    private static final Logger log = LoggerFactory.getLogger(StatsDailyTask.class);

    private static final String TABLE = ArrivalStatsService.STATS_DAILY_TABLE;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private ArrivalStatsService arrivalStatsService;

    /** 回填锁：定时任务与启动回填互斥，避免同一天重复写 */
    private final Object backfillLock = new Object();

    /** 每日 00:05 生成昨日汇总 */
    @Scheduled(cron = "0 5 0 * * ?")
    public void generateYesterday() {
        generateDay(LocalDate.now().minusDays(1));
    }

    /**
     * 启动回填：异步线程把流水上线日~昨日缺失的日汇总补齐（幂等，逐日检测已有行则跳过）。
     * 历史数据不变，回填仅一次成本；服务重启自动续跑。
     */
    @PostConstruct
    public void backfillOnStartup() {
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(10000); // 等上下文预热与数据库就绪
                LocalDate start = resolveStatStartDate();
                LocalDate yesterday = LocalDate.now().minusDays(1);
                if (start == null || start.isAfter(yesterday)) {
                    log.info("日汇总回填无历史区间(统计起始日={})", start);
                    return;
                }
                int generated = 0;
                for (LocalDate d = start; !d.isAfter(yesterday); d = d.plusDays(1)) {
                    if (hasDaily(d)) {
                        continue;
                    }
                    if (generateDay(d)) {
                        generated++;
                    }
                }
                log.info("日汇总回填完成: 区间 {} ~ {}，新增生成 {} 天", start, yesterday, generated);
            } catch (Exception e) {
                log.warn("日汇总回填异常(不影响业务, 明日定时任务续补): {}", e.getMessage());
            }
        }, "stats-daily-backfill");
        t.setDaemon(true);
        t.start();
    }

    /** 生成指定日汇总（幂等覆盖写）；表未建或缺列时告警跳过 */ 
    private boolean generateDay(LocalDate day) {
        synchronized (backfillLock) {
            try {
                ArrivalStatsService.DaySnapshot snap = arrivalStatsService.computeDaySnapshot(day);
                // 先删后插在事务内完成，避免区间查询读到删除与插入之间的半状态
                transactionTemplate.executeWithoutResult(ts -> upsertDay(snap));
                log.info("日汇总已生成: date={}, 站点行={}, 报文={}, 入库={}",
                        day, snap.arrivalBySite.size(), snap.msgRows, snap.storedRows);
                return true;
            } catch (Exception e) {
                log.warn("日汇总生成失败(表未建或缺列, 区间查询历史日将按无数据处理): date={}, {}",
                        day, e.getMessage());
                return false;
            }
        }
    }

    /** 先删后插（幂等，调用方保证事务内执行） */
    private void upsertDay(ArrivalStatsService.DaySnapshot snap) {
        Date d = Date.valueOf(snap.day);
        jdbcTemplate.update("DELETE FROM " + TABLE + " WHERE stats_date = ?", d);

        String sql = "INSERT INTO " + TABLE + " (stats_date, stcd, arrival_expected, arrival_arrived,"
                + " arrival_missed, collect_expected, collect_actual, collect_success, collect_fail,"
                + " msg_rows, stored_rows) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        List<Object[]> batch = new ArrayList<>();

        // 站点行
        for (Map.Entry<String, long[]> e : snap.arrivalBySite.entrySet()) {
            long[] a = e.getValue();
            long[] c = snap.collectBySite.getOrDefault(e.getKey(), new long[]{0, 0, 0, 0});
            batch.add(new Object[]{d, e.getKey(), a[0], a[1], a[2], c[0], c[1], c[2], c[3], 0L, 0L});
        }
        // 维度行
        for (Map.Entry<String, long[]> e : snap.collectByDim.entrySet()) {
            long[] v = e.getValue();
            batch.add(new Object[]{d, "__collect_" + e.getKey(), 0L, 0L, 0L, v[0], v[1], v[2], v[3], 0L, 0L});
        }
        // 全局行
        batch.add(new Object[]{d, "__global__", 0L, 0L, 0L, 0L, 0L, 0L, 0L, snap.msgRows, snap.storedRows});

        jdbcTemplate.batchUpdate(sql, batch);
    }

    /** 该日是否已有汇总行（任意行即可视为已生成） */
    private boolean hasDaily(LocalDate day) {
        try {
            Number n = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM " + TABLE + " WHERE stats_date = ?",
                    Number.class, Date.valueOf(day));
            return n != null && n.longValue() > 0;
        } catch (Exception e) {
            return false; // 表不存在等异常交由 generateDay 报错
        }
    }

    /** 统计起始日（流水上线日）：与统计服务同源探测 */
    private LocalDate resolveStatStartDate() {
        try {
            return arrivalStatsService.resolveStatStartDatePublic();
        } catch (Exception e) {
            log.warn("回填起始日探测失败: {}", e.getMessage());
            return null;
        }
    }
}
