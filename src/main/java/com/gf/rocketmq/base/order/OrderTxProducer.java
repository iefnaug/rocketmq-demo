package com.gf.rocketmq.base.order;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author GF
 * @since 2023/4/12
 */
@Slf4j
public class OrderTxProducer {


    public static void main(String[] args) throws MQClientException, IOException {
        TransactionMQProducer transactionMQProducer = new TransactionMQProducer("order_producer");
        transactionMQProducer.setNamesrvAddr("47.109.31.128:9876");

        Order order = new Order();
        order.setOrderId(1L);
        order.setGoodsId(1L);

        transactionMQProducer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                log.info("message: {},  arg: {}", msg, arg);
                return LocalTransactionState.UNKNOW;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                log.info("check message: {}", msg);
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        });

        transactionMQProducer.start();


        Message message = new Message("order", order.toString().getBytes(StandardCharsets.UTF_8));

        transactionMQProducer.sendMessageInTransaction(message, "test");




        System.in.read();
        transactionMQProducer.shutdown();
    }

}
