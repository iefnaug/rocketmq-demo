package com.gf.rocketmq.base.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author GF
 * @since 2023/4/11
 */
@Slf4j
public class DelayProducer {


    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("producer_delay");
        producer.setNamesrvAddr("47.109.31.128:9876");
        producer.start();

        Message message = new Message("DelayTopic", "It`s a delay message".getBytes());
        message.setDelayTimeLevel(3);

        SendResult send = producer.send(message);
        log.info("send result: {}", send);

    }

}
