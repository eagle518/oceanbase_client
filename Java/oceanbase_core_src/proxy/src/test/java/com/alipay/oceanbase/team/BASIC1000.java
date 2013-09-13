/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2013 All Rights Reserved.
 */
package com.alipay.oceanbase.team;

import java.sql.SQLException;

import org.junit.Test;

/**
 * BASIC1000：测试使用datasouce正常的应用场景。
 * 
 * @author liangjie.li@alipay.com
 * @version $Id: Basic1000.java, v 0.1 2013-3-28 上午10:40:44 liangjie.li Exp $
 */
public class BASIC1000 extends BaseBASIC1 {

    /**
     * 测试configUrl使用环境
     * 测试依赖结果依赖环境 10.232.23.13
     * 
     * @throws SQLException
     */
    @Test
    public void test1() {
        CommonOperation.testSelectCluster(dataSource);
    }

    /**
     * 测试configUrl使用环境
     * 测试依赖结果依赖环境 10.232.23.13
     * 
     * @throws SQLException
     */
    @Test
    public void test2() {
    }

}
