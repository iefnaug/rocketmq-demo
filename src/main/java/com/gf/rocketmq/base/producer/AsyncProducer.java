package com.gf.rocketmq.base.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * @author GF
 * @since 2023/4/10
 */
@Slf4j
public class AsyncProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr("47.109.31.128:9876");
        producer.start();

        for (int i = 0; i < 3 ; i++) {
            Message message = new Message("base", "tag2", ("Hello World " + i).getBytes(StandardCharsets.UTF_8));
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.info("发送成功: {}", sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    log.error("发送失败: {}", throwable.getMessage());
                }
            });
            TimeUnit.SECONDS.sleep(1);
        }

        producer.shutdown();
    }

}
