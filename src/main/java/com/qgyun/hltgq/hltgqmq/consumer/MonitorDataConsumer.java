package com.qgyun.hltgq.hltgqmq.consumer;

import com.qgyun.hltgq.hltgqmq.service.MonitorDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.annotation.Argument;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * MonitorData 队列消息消费者
 */
@Component
public class MonitorDataConsumer {

    private static final Logger log = LoggerFactory.getLogger(MonitorDataConsumer.class);

    @Autowired
    private MonitorDataService monitorDataService;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @PostConstruct
    public void init() {
        log.info("===== MonitorDataConsumer @PostConstruct: Bean 已创建 =====");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        log.info("===== 应用启动完成, 检查 RabbitMQ 监听器状态 =====");
        try {
            java.util.Properties props = amqpAdmin.getQueueProperties("MonitorData");
            if (props != null) {
                log.info("队列 MonitorData 存在: size={}, consumers={}",
                        props.get("QUEUE_MESSAGE_COUNT"), props.get("QUEUE_CONSUMER_COUNT"));
                log.info("队列参数: {}", props);
            } else {
                log.error("!!!!! 队列 MonitorData 不存在或不 accessible !!!!!");
            }
        } catch (Exception e) {
            log.error("检查队列状态失败: {}", e.getMessage());
        }
    }

    /**
     * 监听 MonitorData 队列，接收消息并入库
     * 使用 queuesToDeclare 自包含声明，无需依赖外部 Queue bean
     */
    @RabbitListener(queuesToDeclare = @Queue(
            value = "MonitorData",
            durable = "true",
            arguments = @Argument(name = "x-message-ttl", value = "3600000", type = "java.lang.Long")
    ))
    public void handleMessage(String message) {
        log.info("===== 收到 MonitorData 消息: {} =====", message);
        monitorDataService.process(message);
    }
}
