package com.qgyun.hltgq.hltgqmq.config;

import com.qgyun.hltgq.hltgqmq.service.MqttGateActionService;
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

import java.util.Arrays;

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

    /** 订阅 QoS：1=至少一次（配合 cleanSession=false 可实现 Broker 侧离线消息缓存） */
    @Value("${mqtt.qos:1}")
    private int qos;

    @Value("${mqtt.auto-startup:true}")
    private boolean autoStartup;

    @Autowired
    private MqttGateDataService mqttGateDataService;

    /** 闸门动作记录（DZJL topic）解析入库服务 */
    @Autowired
    private MqttGateActionService mqttGateActionService;

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
        // cleanSession=false：Broker 保留会话，进程重启窗口内 QoS>=1 的消息会被缓存并在重连后补投；
        // 前提是 client-id 必须固定（见 application.properties mqtt.client-id），否则每次重启都会新建会话造成 Broker 会话泄漏
        options.setCleanSession(false);
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
        String[] topicArr = topics.split(",");
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(clientId, mqttClientFactory(), topicArr);
        int[] qosArr = new int[topicArr.length];
        Arrays.fill(qosArr, qos);
        adapter.setQos(qosArr);
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
                // 按 topic 路由：DZJL=闸门动作记录（含兼容层级主题 xxx/DZJL），其余走闸门快照链路
                boolean gateAction = topic != null && (topic.equals("DZJL") || topic.endsWith("/DZJL"));
                if (gateAction) {
                    // DZJL 闸门开启期间每秒一帧，这里不打印收发日志（避免刷屏堵塞回调）；
                    // 动作帧日志由 MqttGateActionService 按“是否新动作”级别输出
                } else {
                    log.info("收到 MQTT 消息: topic={}, payload长度={}", topic, payloadStr.length());
                    // HLTData 报文(约10分钟/条)，完整报文用 debug 级别避免日志刷屏
                    log.debug("MQTT 报文: topic={}, payload={}", topic, payloadStr);
                }
                if (!payloadStr.isEmpty()) {
                    if (gateAction) {
                        mqttGateActionService.process(topic, payloadStr);
                    } else {
                        mqttGateDataService.process(payloadStr);
                    }
                }
            }
        };
    }
}
