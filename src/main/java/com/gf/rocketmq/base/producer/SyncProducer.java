package com.gf.rocketmq.base.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * @author GF
 * @since 2023/4/10
 */
public class SyncProducer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr("47.109.31.128:9876");
        producer.start();

        for (int i = 0; i < 1 ; i++) {
            Message message = new Message("base", "tag1", ("你好 " + i).getBytes(StandardCharsets.UTF_8));
            SendResult result = producer.send(message);
            SendStatus status = result.getSendStatus();
            String msgId = result.getMsgId();
            int queueId = result.getMessageQueue().getQueueId();
            System.out.println("发送结果：" + result);
//            System.out.println("发送状态：" + status + " 消息id:" + msgId + "队列id:" + queueId);
//            TimeUnit.SECONDS.sleep(1);
        }

        producer.shutdown();
    }

}
