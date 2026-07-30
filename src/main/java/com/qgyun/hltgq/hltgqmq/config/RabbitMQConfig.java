package com.qgyun.hltgq.hltgqmq.config;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ 配置类
 */
@Configuration
public class RabbitMQConfig {

    /**
     * 声明 MonitorData 队列，参数需与服务器端已有队列完全一致
     * 服务端队列参数: x-message-ttl=3600000（消息过期 1 小时）
     */
    @Bean
    public Queue monitorDataQueue() {
        return QueueBuilder.durable("MonitorData")
                .withArgument("x-message-ttl", 3600000)
                .build();
    }
}
