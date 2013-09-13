package com.alipay.oceanbase.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: OceanbaseDataSourceProxy.java, v 0.1 2013-5-24 上午11:22:15 liangjie.li Exp $
 */
public class CustomerThreadFactory implements ThreadFactory {
    private static final AtomicInteger count = new AtomicInteger(0);

    /**
     * 
     * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
     */
    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName("Oceanbase-AutoUpdate-Thread-" + count.getAndIncrement());
        t.setDaemon(true);
        return t;
    }
}