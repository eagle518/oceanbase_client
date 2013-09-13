/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2013 All Rights Reserved.
 */
package com.alipay.oceanbase.team;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alipay.oceanbase.OceanbaseDataSourceProxy;

/**
 * 
 * @author liangjie.li@alipay.com
 * @version $Id: BaseBASIC2.java, v 0.1 2013-3-28 下午1:47:42 liangjie.li Exp $
 */
public abstract class BaseBASIC2 {

    protected static OceanbaseDataSourceProxy dataSource = null;

    @BeforeClass
    public static void setUp() {
        ApplicationContext context = new ClassPathXmlApplicationContext(
            "classpath:applicationContext.xml");
        dataSource = (OceanbaseDataSourceProxy) context.getBean("oceanbaseDataSourceProxy");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (dataSource != null) {
            dataSource.destroy();
        }
    }
}
