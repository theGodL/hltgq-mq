package com.qgyun.hltgq.hltgqmq.config;

import com.qgyun.hltgq.hltgqmq.service.MqttGateDataService;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

/**
 * MQTT 客户端配置（与 RabbitMQ 并行运行）
 */
@Configuration
public class MqttConfig {

    private static final Logger log = LoggerFactory.getLogger(MqttConfig.class);

    @Value("${mqtt.url}")
    private String mqttUrl;

    @Value("${mqtt.client-id}")
    private String clientId;

    @Value("${mqtt.username}")
    private String username;

    @Value("${mqtt.password}")
    private String password;

    @Value("${mqtt.topics}")
    private String topics;

    @Value("${mqtt.auto-startup:true}")
    private boolean autoStartup;

    @Autowired
    private MqttGateDataService mqttGateDataService;

    /**
     * MQTT 客户端工厂
     */
    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[]{mqttUrl});
        options.setUserName(username);
        options.setPassword(password.toCharArray());
        options.setCleanSession(true);
        options.setConnectionTimeout(5);
        options.setKeepAliveInterval(60);
        factory.setConnectionOptions(options);
        return factory;
    }

    /**
     * MQTT 消息输入通道
     */
    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    /**
     * MQTT 入站适配器（订阅指定topic）
     */
    @Bean
    public MqttPahoMessageDrivenChannelAdapter mqttInbound() {
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(clientId, mqttClientFactory(), topics.split(","));
        adapter.setCompletionTimeout(5000);
        adapter.setOutputChannel(mqttInputChannel());
        adapter.setAutoStartup(autoStartup);
        return adapter;
    }

    /**
     * MQTT 消息处理器
     */
    @Bean
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public MessageHandler mqttMessageHandler() {
        return new MessageHandler() {
            @Override
            public void handleMessage(Message<?> message) {
                String topic = (String) message.getHeaders().get(MqttHeaders.RECEIVED_TOPIC);
                Object payload = message.getPayload();
                String payloadStr = payload != null ? payload.toString() : "";
                log.info("收到 MQTT 消息: topic={}, payload长度={}", topic, payloadStr.length());
                // MQTT 高频数据(约5s/条)，完整报文用 debug 级别避免日志刷屏
                log.debug("MQTT 报文: topic={}, payload={}", topic, payloadStr);
                if (!payloadStr.isEmpty()) {
                    mqttGateDataService.process(payloadStr);
                }
            }
        };
    }
}
