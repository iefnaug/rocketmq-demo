package com.gf.rocketmq.boot.simple;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * @author GF
 * @since 2023/4/12
 */
@Slf4j
@RocketMQMessageListener(topic = "topic_simple", selectorType = SelectorType.TAG, selectorExpression = "tag1",consumerGroup = "consumer_simple")
@Component
public class SimpleConsumer implements RocketMQListener<String> {


    @Override
    public void onMessage(String message) {
        log.info("message: {}", message);
    }


}
