package com.gf.rocketmq.origin.time;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * @author gf01867832
 * @since 2024/7/17
 */
public class TimerMessageProducer {

    public static final String PRODUCER_GROUP = "TimerMessageProducerGroup";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TimerTopic";


    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        producer.start();

        Message msg = new Message(TOPIC, "测试延迟消息".getBytes(StandardCharsets.UTF_8));
        msg.setDeliverTimeMs(System.currentTimeMillis() + 5_000L);
        SendResult sendResult = producer.send(msg);
        System.out.printf("sendResult: %s", JSONObject.toJSONString(sendResult));

        producer.shutdown();
    }
}