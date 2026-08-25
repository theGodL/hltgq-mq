package com.qgyun.hltgq.hltgqmq.controller;

import com.qgyun.hltgq.hltgqmq.service.RecallService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 召测接口：触发向现场 RTU 下发召测指令（经 RabbitMQ 召测队列转发给上游报文服务）。
 * 目前仅支持 37H 最新实时数据召测，38H 时段数据暂未开放。
 * <p>
 * 同步等待模式：接口阻塞等待 RTU 应答加报入库完成（默认超时 360 秒，配置 recall.timeout-seconds，设备方反馈 RTU 应答最长 5 分钟），
 * 返回 code=0 即代表最新数据已入库，展示层可直接刷新。
 * 另提供状态查询（刷新页面恢复按钮状态）与确认复位接口。
 * <p>
 * 请求体示例：
 * <pre>
 * {"stcd":"9000000005","afn":"37"}
 * </pre>
 */
@RestController
@RequestMapping("/api/recall")
public class RecallController {

    private static final Logger log = LoggerFactory.getLogger(RecallController.class);

    @Autowired
    private RecallService recallService;

    /**
     * 触发召测（同步等待 RTU 应答入库完成后返回）
     */
    @PostMapping
    public Map<String, Object> recall(@RequestBody Map<String, String> body) {
        if (body == null || body.get("stcd") == null || body.get("stcd").trim().isEmpty()) {
            return error("缺少必填参数 stcd（遥测站码）");
        }
        String afn = body.get("afn");
        if (afn == null || afn.trim().isEmpty()) {
            return error("缺少必填参数 afn（37/38）");
        }
        try {
            return recallService.recall(body.get("stcd").trim(), afn.trim(),
                    body.get("stime"), body.get("etime"));
        } catch (Exception e) {
            log.error("召测处理异常: stcd={}, afn={}", body.get("stcd"), afn, e);
            return error("召测处理异常: " + e.getMessage());
        }
    }

    /**
     * 查询该站召测按钮状态（展示层刷新页面后调用，恢复按钮状态）。
     * data.status：RECALLING=召测中(灰) / CONFIRMED=数据确认(绿，待用户点击确认) / IDLE=空闲(可召测)。
     *
     * @param stcd 遥测站码
     */
    @GetMapping("/status")
    public Map<String, Object> status(@RequestParam("stcd") String stcd) {
        if (stcd == null || stcd.trim().isEmpty()) {
            return error("缺少必填参数 stcd（遥测站码）");
        }
        try {
            return recallService.status(stcd.trim());
        } catch (Exception e) {
            log.error("查询召测状态异常: stcd={}", stcd, e);
            return error("查询召测状态异常: " + e.getMessage());
        }
    }

    /**
     * 用户点击"数据确认"按钮后调用：清除该站召测成功记录，按钮复位为召测样式。
     *
     * @param stcd 遥测站码
     */
    @PostMapping("/confirm")
    public Map<String, Object> confirm(@RequestParam("stcd") String stcd) {
        if (stcd == null || stcd.trim().isEmpty()) {
            return error("缺少必填参数 stcd（遥测站码）");
        }
        try {
            return recallService.confirm(stcd.trim());
        } catch (Exception e) {
            log.error("召测确认异常: stcd={}", stcd, e);
            return error("召测确认异常: " + e.getMessage());
        }
    }

    private Map<String, Object> error(String msg) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", 1);
        result.put("msg", msg);
        return result;
    }
}
