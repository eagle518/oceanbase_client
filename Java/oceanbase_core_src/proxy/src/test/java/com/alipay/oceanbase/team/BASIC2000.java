/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2013 All Rights Reserved.
 */
package com.alipay.oceanbase.team;

import org.junit.Test;

/**
 * 
 * @author liangjie.li@alipay.com
 * @version $Id: BASIC2000.java, v 0.1 2013-3-28 下午1:47:59 liangjie.li Exp $
 */
public class BASIC2000 extends BaseBASIC2 {

    @Test
    public void test1() {
        CommonOperation.testSelectCluster(dataSource);
    }
}
