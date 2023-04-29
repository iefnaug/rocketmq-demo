package com.gf.rocketmq.base.producer;

import com.gf.rocketmq.base.order.OrderStep;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.IOException;
import java.util.List;

/**
 * 顺序消息
 * @author GF
 * @since 2023/4/11
 */
@Slf4j
public class OrderProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException, IOException {
        DefaultMQProducer producer = new DefaultMQProducer("Order_Topic_Producer");
        producer.setNamesrvAddr("47.109.31.128:9876");
        producer.start();

        List<OrderStep> orderSteps = OrderStep.buildOrders();
        for (OrderStep orderStep : orderSteps) {
            Message message = new Message("OrderTopic", "Order", orderStep.toString().getBytes());
            producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    long orderId = (long) o;
                    int index = (int) (orderId % list.size());
                    return list.get(index);
                }
            }, orderStep.getOrderId());
        }

        log.info("消息发送完成");
        System.in.read();
    }

}
