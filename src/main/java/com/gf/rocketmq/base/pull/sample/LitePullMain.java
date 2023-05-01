package com.gf.rocketmq.base.pull.sample;

import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author GF
 * @since 2023/5/1
 */
public class LitePullMain {

    public static void main(String[] args) {

        String consumerGroup = "dw_test_consumer_group";
        String nameserverAddr = "47.109.31.128:9876";
        String topic = "dw_test";
        String filter = "*";
        /** 创建调度任务线程池 */
        ScheduledExecutorService schedule = new ScheduledThreadPoolExecutor(1, new
                DefaultThreadFactory("main-schedule-"));
        schedule.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                BigDataPullConsumer demoMain = new BigDataPullConsumer(consumerGroup, nameserverAddr, topic,
                        filter);
                demoMain.registerMessageListener(new BigDataPullConsumer.MessageListener() {
                    /**
                     * 业务处理
                     * @param msgs
                     * @return
                     */
                    @Override
                    public boolean consumer(List<MessageExt> msgs) {
                        System.out.println("本次处理的消息条数：" + msgs.size());
                        return true;
                    }
                });
                demoMain.start();
                demoMain.stop();
            }
        }, 1000, 30 * 1000, TimeUnit.MILLISECONDS);

        try {
            CountDownLatch cdh = new CountDownLatch(1);
            cdh.await(10 , TimeUnit.MINUTES);
            schedule.shutdown();
        } catch (Throwable e) {
            //ignore
        }

    }

}
