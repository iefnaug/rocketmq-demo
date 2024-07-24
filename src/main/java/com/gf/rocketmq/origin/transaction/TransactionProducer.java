package com.gf.rocketmq.origin.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author gf01867832
 * @since 2024/7/24
 */
public class TransactionProducer {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        TransactionListener transactionListener = new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                //执行本地事务
                System.out.println("执行本地事务，参数：" + new String(msg.getBody(), StandardCharsets.UTF_8));
                //位置状态等待消息check
                return LocalTransactionState.UNKNOW;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println("检查本地事务, msg = " + new String(msg.getBody(), StandardCharsets.UTF_8));
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        };
        TransactionMQProducer producer = new TransactionMQProducer("transaction_producer", Arrays.asList("TopicTransaction"));

        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2000), r -> {
            Thread thread = new Thread(r);
            thread.setName("client-transaction-msg-check-thread " + thread.getId());
            return thread;
        });

        producer.setExecutorService(executorService);
        producer.setTransactionListener(transactionListener);
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 1; i++) {
            try {
                Message msg =
                        new Message("TopicTransaction", tags[i % tags.length], "KEY" + i,
                                ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.sendMessageInTransaction(msg, "transaction test!");
                System.out.printf("%s%n", sendResult);

                Thread.sleep(10);
            } catch (
                    MQClientException |
                    UnsupportedEncodingException e) {
                e.printStackTrace();
            } catch (
                    InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        for (int i = 0; i < 10000; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();
    }

}
