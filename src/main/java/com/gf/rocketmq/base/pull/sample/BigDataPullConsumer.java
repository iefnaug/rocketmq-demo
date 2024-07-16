package com.gf.rocketmq.base.pull.sample;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author GF
 * @since 2023/5/1
 */
public class BigDataPullConsumer {

    private final ExecutorService executorService = new ThreadPoolExecutor(30, 30, 0L,
            TimeUnit.SECONDS, new ArrayBlockingQueue<>(10000), new DefaultThreadFactory("business-executor-"));

    private final ExecutorService pullTaskExecutor = new ThreadPoolExecutor(1, 1, 0L,
            TimeUnit.SECONDS, new ArrayBlockingQueue<>(10), new DefaultThreadFactory("pull-batch-"));

    private String consumerGroup;
    private String nameserverAddr;
    private String topic;
    private String filter;
    private MessageListener messageListener;
    private DefaultMQProducer rertyMQProducer;
    private PullBatchTask pullBatchTask;

    public BigDataPullConsumer(String consumerGroup, String nameserverAddr, String topic, String filter) {
        this.consumerGroup = consumerGroup;
        this.nameserverAddr = nameserverAddr;
        this.topic = topic;
        this.filter = filter;
        initRetryMQProducer();
    }

    private void initRetryMQProducer() {
        this.rertyMQProducer = new DefaultMQProducer(consumerGroup + "-retry");
        this.rertyMQProducer.setNamesrvAddr(this.nameserverAddr);
        try {
            this.rertyMQProducer.start();
        } catch (Throwable e) {
            throw new RuntimeException("启动失败", e);
        }

    }

    public void registerMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public void start() {
        //没有考虑重复调用问题
        this.pullBatchTask = new PullBatchTask(consumerGroup, nameserverAddr, topic,filter,messageListener);
        pullTaskExecutor.submit(this.pullBatchTask);
    }

    public void stop() {
        while(this.pullBatchTask.isRunning()) {
            try {
                Thread.sleep(1 * 1000);
            } catch (Throwable e) {
                //ignore
            }
        }
        this.pullBatchTask.stop();
        pullTaskExecutor.shutdown();
        executorService.shutdown();
        try {
            //等待重试任务结束
            while(executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                this.rertyMQProducer.shutdown();
                break;
            }
        } catch (Throwable e) {
            //igonre
        }
    }

    /**
     * 任务监听
     */
    static interface MessageListener {
        boolean consumer(List<MessageExt> msgs);
    }

    /**
     * 定时调度任务，例如每 10 分钟会被调度一次
     */
    class PullBatchTask implements Runnable {
        DefaultLitePullConsumer consumer;
        String consumerGroup;
        String nameserverAddr;
        String topic;
        String filter;
        private volatile boolean running = true;
        private MessageListener messageListener;

        public PullBatchTask(String consumerGroup, String nameserverAddr,String topic, String filter,
                             MessageListener messageListener) {
            this.consumerGroup = consumerGroup;
            this.nameserverAddr = nameserverAddr;
            this.topic = topic;
            this.filter = filter;
            this.messageListener = messageListener;
            init();
        }

        private void init() {
            System.out.println("init 方法被调用");
            consumer = new DefaultLitePullConsumer(this.consumerGroup);
            consumer.setNamesrvAddr(this.nameserverAddr);
            consumer.setAutoCommit(true);
            consumer.setMessageModel(MessageModel.CLUSTERING);
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.setPullBatchSize(100);
            try {
                consumer.subscribe(topic, filter);
                consumer.start();
            } catch (Throwable e) {
                e.printStackTrace();
            }

        }

        public void stop() {
            this.running = false;
            this.consumer.shutdown();
        }

        public boolean isRunning() {
            return this.running;
        }

        @Override
        public void run() {
            this.running = true;
            long startTime = System.currentTimeMillis() - 5 * 1000;
            System.out.println("run 方法被调用");
            int notFoundMsgCount = 0;

            while(running) {
                try {
                    // 拉取一批消息
                    List<MessageExt> messageExts = consumer.poll();
                    if(messageExts != null && !messageExts.isEmpty()) {
                        notFoundMsgCount = 0;//查询到数据，重置为 0；
                        // 使用一个业务线程池专门消费消息
                        try {
                            executorService.submit(new ExecuteTask(messageExts, messageListener));
                        } catch (RejectedExecutionException e) { //如果被拒绝，停止拉取，业务代码不去拉取，在
                            // RocketMQ 内部会最终也会触发限流，不会再拉取更多的消息，确保不会触发内存溢出。
                            boolean retry = true;
                            while (retry)
                                try {
                                    Thread.sleep(5 * 1000);//简单的限流
                                    executorService.submit(new ExecuteTask(messageExts, messageListener));
                                    retry = false;
                                } catch (RejectedExecutionException e2) {
                                    retry = true;
                                }
                        }

                        MessageExt last = messageExts.get(messageExts.size() - 1);
                        /**
                         * 如果消息处理的时间超过了该任务的启动时间，本次批处理就先结束
                         * 停掉该消费者之前，建议先暂停拉取，这样就不会从 broker 中拉取消息
                         * */
                        if(last.getStoreTimestamp() > startTime) {
                            System.out.println("consumer.pause 方法将被调用。");
                            consumer.pause(buildMessageQueues(last));
                        }

                    } else {
                        notFoundMsgCount ++;
                    }

                    //如果连续出现 5 次未拉取到消息，说明本地缓存的消息全部处理，并且 pull 线程已经停止拉取了,此时可以结束本次消
                    //息拉取，等待下一次调度任务
                    if(notFoundMsgCount > 5) {
                        System.out.println("已连续超过 5 次未拉取到消息，将退出本次调度");
                        break;
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
            this.running = false;
        }

        /**
         * 构建 MessageQueue
         * @param msg
         * @return
         */
        private Set<MessageQueue> buildMessageQueues(MessageExt msg) {
            Set<MessageQueue> queues = new HashSet<>();
            MessageQueue queue = new MessageQueue(msg.getTopic(), msg.getBrokerName(), msg.getQueueId());
            queues.add(queue);
            return queues;
        }
    }

    /**
     * 任务执行
     */
    class ExecuteTask implements Runnable {
        private List<MessageExt> msgs;
        private MessageListener messageListener;
        public ExecuteTask(List<MessageExt> allMsgs, MessageListener messageListener) {
            this.msgs = allMsgs.stream().filter((MessageExt msg) -> msg.getReconsumeTimes() <=
                    16).collect(Collectors.toList());
            this.messageListener = messageListener;
        }
        @Override
        public void run() {
            try {
                this.messageListener.consumer(this.msgs);
            } catch (Throwable e) {
                //消息消费失败，需要触发重试
                //这里可以参考 PUSH 模式，将消息再次发送到服务端。
                try {
                    for(MessageExt msg : this.msgs) {
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        rertyMQProducer.send(msg);
                    }
                } catch (Throwable e2) {
                    e2.printStackTrace();
                    // todo 重试
                }
            }
        }
    }

}
