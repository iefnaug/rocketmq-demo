package com.gf.rocketmq.base.pull;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * @author GF
 * @since 2023/4/10
 */
@Slf4j
public class PullProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("pull_producer_group");
        producer.setNamesrvAddr("47.109.31.128:9876");
        producer.start();

        for (int i = 0; i < 200 ; i++) {
            Message message = new Message("topic_pull", null, ("你好 " + i).getBytes(StandardCharsets.UTF_8));
            SendResult result = producer.send(message);
            log.info("发送结果：{}", result);
        }

        producer.shutdown();
    }

}
