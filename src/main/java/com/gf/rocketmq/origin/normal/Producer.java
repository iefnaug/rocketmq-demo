package com.gf.rocketmq.origin.normal;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.UUID;

/**
 * @since 2024/7/16
 */
public class Producer {

    public static final int MESSAGE_COUNT = 2;
    public static final String PRODUCER_GROUP = "please_rename_unique_group_name";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "TopicTest";
    public static final String TAG = "TagA";


    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        producer.setRetryTimesWhenSendFailed(2);
        producer.start();
//        for (int i = 0; i < MESSAGE_COUNT; i++) {
//            Message message = new Message();
//            message.setTopic(TOPIC);
//            message.setTags(TAG);
//            message.setKeys(UUID.randomUUID().toString());
//            message.setBody(("Hello Rocketmq + " + i).getBytes(StandardCharsets.UTF_8));
//            //普通同步发送
//            SendResult sendResult = producer.send(message);
//            System.out.printf("%s%n", JSONObject.toJSONString(sendResult));
            //不关心发送结果
//            producer.sendOneway(message);
            //异步发送
//            producer.send(message, new SendCallback() {
//                @Override
//                public void onSuccess(SendResult sendResult) {
//                    System.out.printf("async sendResult %s%n", JSONObject.toJSONString(sendResult));
//                }
//
//                @Override
//                public void onException(Throwable e) {
//                    System.out.printf("error %s%n", e.getMessage());
//                }
//            });
//        }
        while (true) {
            System.out.print("input message: ");
            Scanner scanner = new Scanner(System.in);
            String line = scanner.nextLine();
            if (StringUtils.equals(line, "q") || StringUtils.isBlank(line)) {
                break;
            } else {
                Message message = new Message();
                message.setTopic(TOPIC);
//                message.setTags(TAG);
                message.setTags("TagB");
                message.putUserProperty("name", "k");
                message.setKeys(UUID.randomUUID().toString());
                message.setBody(line.getBytes(StandardCharsets.UTF_8));
                SendResult sendResult = producer.send(message);
                System.out.println(JSONObject.toJSONString(sendResult));
            }
        }
        producer.shutdown();
    }

}
