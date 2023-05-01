package com.gf.rocketmq.base.pull;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author GF
 * @since 2023/4/10
 */
@Slf4j
public class PullConsumer {

    public static void main(String[] args) throws MQClientException {

        subscribe();
//        assign();
    }

    /**
     * 订阅模式，自动提交点位
     */
    public static void subscribe() throws MQClientException {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("pull_consumer");
        consumer.setNamesrvAddr("47.109.31.128:9876");
        consumer.subscribe("topic_pull", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setAutoCommit(false);
        consumer.setPullBatchSize(1000);
//        consumer.setAutoCommit(true);
        consumer.start();

        while (true) {
            List<MessageExt> messageExtList = consumer.poll();
            log.info("size: {}", messageExtList.size());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            doSomething(messageExtList);
        }
    }


    public static void assign() throws MQClientException {
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("pull_consumer");
        consumer.setNamesrvAddr("47.109.31.128:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setAutoCommit(false);
        consumer.start();

        Collection<MessageQueue> messageQueues = consumer.fetchMessageQueues("topic_pull");
        ArrayList<MessageQueue> queues = new ArrayList<>(messageQueues);
        //指定拉取的队列
        consumer.assign(queues.subList(0, 2));
        consumer.seek(queues.get(1), 0);


        while (true) {
            List<MessageExt> messageExtList = consumer.poll();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            doSomething(messageExtList);
            //提交点位
            consumer.commitSync();
        }
    }


    private static void doSomething(List<MessageExt> messageExtList) {
        for (MessageExt messageExt : messageExtList) {
            log.info("size: {}, body: {}", messageExtList.size(), new String(messageExt.getBody(), StandardCharsets.UTF_8));
        }
    }

}
