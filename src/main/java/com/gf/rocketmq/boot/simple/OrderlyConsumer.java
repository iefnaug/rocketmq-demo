package com.gf.rocketmq.boot.simple;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author GF
 * @since 2023/4/12
 */
@Slf4j
@Component
@RocketMQMessageListener(topic = "topic_orderly", consumeMode = ConsumeMode.ORDERLY, consumerGroup = "consumer_orderly")
public class OrderlyConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String message) {
        log.info("message: {}", message);
        if (message.contains("id=0")) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
