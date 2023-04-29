package com.gf.rocketmq.boot.simple;

import com.gf.rocketmq.boot.entity.SimpleMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author GF
 * @since 2023/4/12
 */
@Component
@Slf4j
public class SimpleProducer {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    public void sendMsg() {
        log.info("send message begin...");
//        SimpleMsg msg = new SimpleMsg(1L, "hello");

//        rocketMQTemplate.syncSend("topic_simple:tag1", MessageBuilder.withPayload(msg.toString()).setHeader("KEYS", "k2").build());
//        rocketMQTemplate.syncSend("topic_simple:tag1", MessageBuilder.withPayload(msg.toString()).setHeader("KEYS", "k2").build(), 200, 3);

//
//        rocketMQTemplate.asyncSend("topic_simple:tag1", MessageBuilder.withPayload(msg.toString()).setHeader("KEYS", "k3").build(), new SendCallback() {
//            @Override
//            public void onSuccess(SendResult sendResult) {
//
//            }
//
//            @Override
//            public void onException(Throwable e) {
//
//            }
//        });

        for (int i = 0; i < 10; i++) {
            SimpleMsg msg = new SimpleMsg((long) i, "hello" + i);
            SendResult sendResult = rocketMQTemplate.syncSendOrderly("topic_orderly:tag1", MessageBuilder.withPayload(msg.toString()).build(), (i % 2) == 0 ? "a" : "d");
//            log.info("send info: {}", sendResult);
//            rocketMQTemplate.syncSend("topic_simple:tag1", MessageBuilder.withPayload(msg.toString()).setHeader("KEYS", "k2").build());
//            rocketMQTemplate.asyncSend("topic_simple:tag1", MessageBuilder.withPayload(msg.toString()).setHeader("KEYS", "k3").build(), new SendCallback() {
//                @Override
//                public void onSuccess(SendResult sendResult) {
//
//                }
//
//                @Override
//                public void onException(Throwable e) {
//
//                }
//            });
        }

    }

    public static void main(String[] args) {
        System.out.println("a".hashCode());
        System.out.println("d".hashCode());
    }

}
