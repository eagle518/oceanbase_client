package com.alipay.oceanbase.util.thread;

import static org.powermock.api.mockito.PowerMockito.mock;

import org.junit.Test;

import com.alipay.oceanbase.util.thread.CustomerThreadFactory;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: CustomerThreadFactoryTest.java, v 0.1 Jun 19, 2013 5:45:47 PM liangjie.li Exp $
 */
public class CustomerThreadFactoryTest {

    @Test
    public void testNewThread() {
        Runnable mockRunnable = mock(Runnable.class);

        CustomerThreadFactory ctf = new CustomerThreadFactory();
        Thread t = ctf.newThread(mockRunnable);
        t.getName().equals("Oceanbase-Datasource-Daemon-Thread1");

        Thread _t = ctf.newThread(mockRunnable);
        _t.getName().equals("Oceanbase-Datasource-Daemon-Thread2");

    }
}
