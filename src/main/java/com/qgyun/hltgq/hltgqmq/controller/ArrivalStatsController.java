package com.qgyun.hltgq.hltgqmq.controller;

import com.qgyun.hltgq.hltgqmq.service.ArrivalStatsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 统计接口：统计大屏数据。
 * <p>
 * 所有接口支持可选日期区间参数 startDate/endDate（yyyy-MM-dd，含两端）：
 * 缺省=今日；只传 startDate=该日至今日；只传 endDate=该单日。
 * 区间上限 {@link ArrivalStatsService#RANGE_MAX_DAYS} 天；miss-detail 因窗级明细需实时计算，
 * 单独限 {@link ArrivalStatsService#MISS_DETAIL_MAX_DAYS} 天。
 * <p>
 * GET /api/report/arrival-stats  统计卡片：区间到报率/区间平均到报率/区间缺测率/未到报站点/缺测站点
 * GET /api/report/arrival-detail 站点到报明细：站点名称/编号/报文类型(整点报|分钟报)/应到/实到/缺报次数/到报率
 * GET /api/report/miss-detail    缺测明细：站点/缺测时间段/缺测时长/数据类型/当前状态(缺测中|已恢复)
 * GET /api/report/collect-stats  数据采集状态统计：水位/流量/雨量/闸门开度/墒情的应采/实采/成功/失败/成功率/失败率
 * GET /api/report/service-status mq 自身服务状态：进程指标 + 接收/解析/存储三服务区间处理量与成功率
 * <p>
 * 状态档位（正常/偏低/异常/未到报）阈值由产品定义；信息发布/视频/告警推送等数据由 hltgq-site / hltgq-device 提供。
 */
@RestController
@RequestMapping("/api/report")
public class ArrivalStatsController {

    private static final Logger log = LoggerFactory.getLogger(ArrivalStatsController.class);

    @Autowired
    private ArrivalStatsService arrivalStatsService;

    /** 统计卡片聚合 */
    @GetMapping("/arrival-stats")
    public Map<String, Object> arrivalStats(@RequestParam(required = false) String startDate,
                                            @RequestParam(required = false) String endDate) {
        try {
            LocalDate[] range = parseRange(startDate, endDate);
            if (range == null) return error("日期参数格式非法，应为 yyyy-MM-dd");
            String rangeErr = validateRange(range[0], range[1], false);
            if (rangeErr != null) return error(rangeErr);
            return ok(range == TODAY_RANGE ? arrivalStatsService.getStats()
                    : arrivalStatsService.getStats(range[0], range[1]));
        } catch (Exception e) {
            log.error("到报率统计异常", e);
            return error("统计查询异常: " + e.getMessage());
        }
    }

    /** 站点到报明细 */
    @GetMapping("/arrival-detail")
    public Map<String, Object> arrivalDetail(@RequestParam(required = false) String startDate,
                                             @RequestParam(required = false) String endDate) {
        try {
            LocalDate[] range = parseRange(startDate, endDate);
            if (range == null) return error("日期参数格式非法，应为 yyyy-MM-dd");
            String rangeErr = validateRange(range[0], range[1], false);
            if (rangeErr != null) return error(rangeErr);
            return ok(range == TODAY_RANGE ? arrivalStatsService.getArrivalDetail()
                    : arrivalStatsService.getArrivalDetail(range[0], range[1]));
        } catch (Exception e) {
            log.error("站点到报明细查询异常", e);
            return error("到报明细查询异常: " + e.getMessage());
        }
    }

    /** 缺测明细（窗级实时计算，区间限 31 天） */
    @GetMapping("/miss-detail")
    public Map<String, Object> missDetail(@RequestParam(required = false) String startDate,
                                          @RequestParam(required = false) String endDate) {
        try {
            LocalDate[] range = parseRange(startDate, endDate);
            if (range == null) return error("日期参数格式非法，应为 yyyy-MM-dd");
            String rangeErr = validateRange(range[0], range[1], true);
            if (rangeErr != null) return error(rangeErr);
            return ok(range == TODAY_RANGE ? arrivalStatsService.getMissDetail()
                    : arrivalStatsService.getMissDetail(range[0], range[1]));
        } catch (Exception e) {
            log.error("缺测明细查询异常", e);
            return error("缺测明细查询异常: " + e.getMessage());
        }
    }

    /** 数据采集状态统计 */
    @GetMapping("/collect-stats")
    public Map<String, Object> collectStats(@RequestParam(required = false) String startDate,
                                            @RequestParam(required = false) String endDate) {
        try {
            LocalDate[] range = parseRange(startDate, endDate);
            if (range == null) return error("日期参数格式非法，应为 yyyy-MM-dd");
            String rangeErr = validateRange(range[0], range[1], false);
            if (rangeErr != null) return error(rangeErr);
            return ok(range == TODAY_RANGE ? arrivalStatsService.getCollectStats()
                    : arrivalStatsService.getCollectStats(range[0], range[1]));
        } catch (Exception e) {
            log.error("采集状态统计异常", e);
            return error("采集状态统计异常: " + e.getMessage());
        }
    }

    /** mq 自身服务状态 */
    @GetMapping("/service-status")
    public Map<String, Object> serviceStatus(@RequestParam(required = false) String startDate,
                                             @RequestParam(required = false) String endDate) {
        try {
            LocalDate[] range = parseRange(startDate, endDate);
            if (range == null) return error("日期参数格式非法，应为 yyyy-MM-dd");
            String rangeErr = validateRange(range[0], range[1], false);
            if (rangeErr != null) return error(rangeErr);
            return ok(range == TODAY_RANGE ? arrivalStatsService.getServiceStatus()
                    : arrivalStatsService.getServiceStatus(range[0], range[1]));
        } catch (Exception e) {
            log.error("服务状态查询异常", e);
            return error("服务状态查询异常: " + e.getMessage());
        }
    }

    /** 今日默认区间哨兵：控制器无参时走原今日路径（与历史接口完全一致） */
    private static final LocalDate[] TODAY_RANGE = new LocalDate[0];

    /** 解析可选日期参数：均缺省返回 TODAY_RANGE 哨兵（走今日路径），任一给出则补全另一端 */
    private LocalDate[] parseRange(String startDate, String endDate) {
        if ((startDate == null || startDate.trim().isEmpty())
                && (endDate == null || endDate.trim().isEmpty())) {
            return TODAY_RANGE;
        }
        try {
            LocalDate today = LocalDate.now();
            LocalDate start = (startDate != null && !startDate.trim().isEmpty())
                    ? LocalDate.parse(startDate.trim()) : today;
            LocalDate end = (endDate != null && !endDate.trim().isEmpty())
                    ? LocalDate.parse(endDate.trim()) : today;
            return new LocalDate[]{start, end};
        } catch (Exception e) {
            return null;
        }
    }

    /** 区间校验：start<=end、总天数上限、miss-detail 额外 31 天上限 */
    private String validateRange(LocalDate start, LocalDate end, boolean missDetail) {
        if (start.isAfter(end)) {
            return "startDate 不能晚于 endDate";
        }
        long days = ChronoUnit.DAYS.between(start, end) + 1;
        if (days > ArrivalStatsService.RANGE_MAX_DAYS) {
            return "查询区间超过上限 " + ArrivalStatsService.RANGE_MAX_DAYS + " 天";
        }
        if (missDetail && days > ArrivalStatsService.MISS_DETAIL_MAX_DAYS) {
            return "缺测明细最多查询 " + ArrivalStatsService.MISS_DETAIL_MAX_DAYS + " 天";
        }
        return null;
    }

    private Map<String, Object> ok(Object data) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", 0);
        result.put("msg", "成功");
        result.put("data", data);
        return result;
    }

    private Map<String, Object> error(String msg) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", 1);
        result.put("msg", msg);
        return result;
    }
}
