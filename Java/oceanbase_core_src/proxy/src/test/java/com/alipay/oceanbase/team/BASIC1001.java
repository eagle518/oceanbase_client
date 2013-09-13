/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2013 All Rights Reserved.
 */
package com.alipay.oceanbase.team;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * BASIC1001：测试使用datasouce升级的应用场景。
 * 
 * @author liangjie.li@alipay.com
 * @version $Id: Basic1000.java, v 0.1 2013-3-28 上午10:40:44 liangjie.li Exp $
 */
public class BASIC1001 extends BaseBASIC1 {

    /**
     * 测试configUrl使用环境
     * 测试依赖结果依赖环境 10.232.23.13
     * @throws Exception 
     */
    /*@Test
    public void test1() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(21);

        for (int i = 0; i < 20; i++) {
            Thread thread = new Thread(new SelectTask(barrier, dataSource1));
            thread.setDaemon(true);
            thread.start();
        }
        Thread thread = new Thread(new SwitchTask(barrier));
        thread.setDaemon(true);
        thread.start();

        TimeUnit.MINUTES.sleep(120);
    }*/

    @Test
    public void test2() throws Exception {

        Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement();
        stmt.executeQuery("select * from __all_server");
        stmt.close();
        connection.close();
        super.tearDown();

        for (int i = 1; i < 10000000; i++) {
            super.setUp();

            Connection connection1 = dataSource.getConnection();
            Statement stmt1 = connection1.createStatement();
            stmt1.executeQuery("select * from __all_server");
            stmt1.close();
            connection1.close();

            super.tearDown();
        }
    }

}

class SwitchTask implements Runnable {
    private CyclicBarrier cb = null;

    public SwitchTask(CyclicBarrier cb) {
        this.cb = cb;
    }

    @Override
    public void run() {
        try {
            cb.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        String sql1 = "update config_data set current_version = '0.0.1', is_process = 1, percentage = 100 where data_id = 'testcase_config_1'";
        String sql2 = "update config_data set current_version = '0.0.2', is_process = 1, percentage = 100 where data_id = 'testcase_config_1'";

        Connection connection = null;
        Statement stmt = null;
        try {
            connection = DriverManager.getConnection(
                "jdbc:mysql://oceanbase.mysql.rds.aliyuncs.com:3331/oceanbase", "oceanbase",
                "oceanbase12341234");
            stmt = connection.createStatement();
        } catch (SQLException e2) {
        }

        int i = 0;
        while (true) {
            try {
                if (i % 2 == 0) {
                    stmt.executeUpdate(sql1);
                } else {
                    stmt.executeUpdate(sql2);
                }
                i++;
            } catch (SQLException e1) {
            }
            try {
                TimeUnit.MINUTES.sleep(1);
            } catch (InterruptedException e) {
            }
        }
    }
}

/**
 * 
 * 
 * @author liangjie.li@alipay.com
 * @version $Id: BASIC1001.java, v 0.1 2013-3-29 上午11:13:22 liangjie.li Exp $
 */
class SelectTask implements Runnable {

    private static final Logger logger     = Logger.getLogger(SelectTask.class);

    private DataSource          dataSource = null;
    private CyclicBarrier       cb         = null;

    public SelectTask(CyclicBarrier cb, DataSource dataSource) {
        this.cb = cb;
        this.dataSource = dataSource;
    }

    @Override
    public void run() {
        try {
            cb.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        while (true) {
            if (logger.isInfoEnabled()) {
                logger.info("execute select " + Thread.currentThread().getName());
            }
            CommonOperation.testSelectCluster(dataSource);
        }
    }

}