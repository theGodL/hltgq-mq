package com.qgyun.hltgq.hltgqmq.controller;

import com.qgyun.hltgq.hltgqmq.service.ArrivalStatsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 统计接口：统计大屏数据。
 * <p>
 * GET /api/report/arrival-stats  统计卡片：今日到报率/本月平均到报率/今日缺测率/未到报站点/缺测站点
 * GET /api/report/arrival-detail 站点到报明细：站点名称/编号/报文类型(整点报|分钟报)/应到/实到/缺报次数/到报率
 * GET /api/report/miss-detail    缺测明细：站点/缺测时间段/缺测时长/数据类型/当前状态(缺测中|已恢复)
 * GET /api/report/collect-stats  数据采集状态统计：水位/流量/雨量/闸门开度/墒情的应采/实采/成功/失败/成功率/失败率
 * GET /api/report/service-status mq 自身服务状态：进程指标 + 接收/解析/存储三服务今日处理量与成功率
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
    public Map<String, Object> arrivalStats() {
        try {
            return ok(arrivalStatsService.getStats());
        } catch (Exception e) {
            log.error("到报率统计异常", e);
            return error("统计查询异常: " + e.getMessage());
        }
    }

    /** 站点到报明细 */
    @GetMapping("/arrival-detail")
    public Map<String, Object> arrivalDetail() {
        try {
            return ok(arrivalStatsService.getArrivalDetail());
        } catch (Exception e) {
            log.error("站点到报明细查询异常", e);
            return error("到报明细查询异常: " + e.getMessage());
        }
    }

    /** 缺测明细 */
    @GetMapping("/miss-detail")
    public Map<String, Object> missDetail() {
        try {
            return ok(arrivalStatsService.getMissDetail());
        } catch (Exception e) {
            log.error("缺测明细查询异常", e);
            return error("缺测明细查询异常: " + e.getMessage());
        }
    }

    /** 数据采集状态统计 */
    @GetMapping("/collect-stats")
    public Map<String, Object> collectStats() {
        try {
            return ok(arrivalStatsService.getCollectStats());
        } catch (Exception e) {
            log.error("采集状态统计异常", e);
            return error("采集状态统计异常: " + e.getMessage());
        }
    }

    /** mq 自身服务状态 */
    @GetMapping("/service-status")
    public Map<String, Object> serviceStatus() {
        try {
            return ok(arrivalStatsService.getServiceStatus());
        } catch (Exception e) {
            log.error("服务状态查询异常", e);
            return error("服务状态查询异常: " + e.getMessage());
        }
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
