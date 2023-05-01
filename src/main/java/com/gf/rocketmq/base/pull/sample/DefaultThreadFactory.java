package com.gf.rocketmq.base.pull.sample;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author GF
 * @since 2023/5/1
 */
public class DefaultThreadFactory implements ThreadFactory {
    private AtomicInteger num = new AtomicInteger(0);
    private String prefix;

    public DefaultThreadFactory(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(prefix + num.incrementAndGet());
        return t;
    }
}
