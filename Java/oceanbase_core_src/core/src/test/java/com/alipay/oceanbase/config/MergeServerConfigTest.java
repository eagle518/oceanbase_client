package com.alipay.oceanbase.config;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: MergeServerConfigTest.java, v 0.1 Jun 17, 2013 4:28:26 PM liangjie.li Exp $
 */
public class MergeServerConfigTest {

    @Test
    public void testToString() {
        MergeServerConfig msc = new MergeServerConfig("10.1.1.0", 1000L);
        Assert.assertEquals("10.1.1.0:1000", msc.toString());

        msc.setIp("10.1.1.1");
        Assert.assertEquals("10.1.1.1:1000", msc.toString());

        msc.setPort(1001L);
        Assert.assertEquals("10.1.1.1:1001", msc.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor1() {
        new MergeServerConfig("10.1.1.0", 0L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor2() {
        new MergeServerConfig("", 2828L);
    }
}
