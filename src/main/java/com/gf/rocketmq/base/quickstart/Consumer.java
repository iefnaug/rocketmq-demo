package com.gf.rocketmq.base.quickstart;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author GF
 * @since 2023/4/10
 */
@Slf4j
public class Consumer {

    public static void main(String[] args) throws MQClientException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quickstart_consumer_group");
        consumer.setNamesrvAddr("47.109.31.128:9876");
        consumer.subscribe("quickstart_topic", "*");
        //一次从broker拉取消息数量, 默认32
//        consumer.setPullBatchSize(10);
        //最大批量读取消息条数，默认只取一条
//        consumer.setConsumeMessageBatchMaxSize(2);
//        consumer.setMessageModel(MessageModel.CLUSTERING);
//        consumer.setMessageModel(MessageModel.BROADCASTING);

        //消息重试次数
        consumer.setMaxReconsumeTimes(2);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
//                log.info("list.size() : {}", list.size());
                list.forEach(messageExt -> {
                    log.info("body: {}, tags: {}", new String(messageExt.getBody()), messageExt.getTags());
                });
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });
        log.info("clientIP: {}", consumer.getClientIP());
        consumer.start();
    }

}
