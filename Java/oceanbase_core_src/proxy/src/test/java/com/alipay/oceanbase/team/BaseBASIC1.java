/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2013 All Rights Reserved.
 */
package com.alipay.oceanbase.team;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.alipay.oceanbase.OceanbaseDataSourceProxy;
import com.alipay.oceanbase.util.Constants;

/**
 * 
 * @author liangjie.li@alipay.com
 * @version $Id: BaseBASIC.java, v 0.1 2013-3-28 上午10:46:34 liangjie.li Exp $
 */
public abstract class BaseBASIC1 {

    protected static OceanbaseDataSourceProxy dataSource  = null;
    protected static OceanbaseDataSourceProxy dataSource1 = null;

    @BeforeClass
    public static void setUp() throws Exception {
        // 1. 
        dataSource = new OceanbaseDataSourceProxy();
        dataSource.setMaxActive(10);
        dataSource.setMinIdle(2);
        dataSource.setUsername(Constants.USER_NAME);
        dataSource.setPassword(Constants.PASSWORD);
        dataSource.setConfigURL(Constants.CONFIG_URL);
        dataSource.init();

        // 2.
        dataSource1 = new OceanbaseDataSourceProxy();
        dataSource1.setMaxActive(10);
        dataSource1.setMinIdle(2);
        dataSource1.setUsername(Constants.USER_NAME);
        dataSource1.setPassword(Constants.PASSWORD);
        dataSource1.setConfigURL(Constants.CONFIG_URL_1);
        dataSource1.init();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        dataSource.destroy();
        dataSource1.destroy();
    }
}
