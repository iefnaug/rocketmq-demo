package com.gf.rocketmq.base.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author GF
 * @since 2023/4/11
 */
@Slf4j
public class OrderConsumer {


    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_g2");
        consumer.setNamesrvAddr("47.109.31.128:9876");
        consumer.subscribe("OrderTopic", "*");
        consumer.setMaxReconsumeTimes(3); //重试3次
        consumer.setSuspendCurrentQueueTimeMillis(5000); //每次间隔5s，重试期间不会消费后续消息

        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                for (MessageExt messageExt : list) {
                    log.info("顺序消费消息： {} {}", new String(messageExt.getBody()), messageExt);
                }
//                return ConsumeOrderlyStatus.SUCCESS;

//                try {
//                    Thread.sleep(3000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
            }
        });

        consumer.start();
        log.info("消费者启动");
    }

}
