package com.alipay.oceanbase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: OBGroupDateSourceTest.java, v 0.1 Jun 20, 2013 12:27:13 PM liangjie.li Exp $
 */
public class OBGroupDateSourceTest {

    @Test
    public void test() throws SQLException, InterruptedException {

        OBGroupDataSource obGroupDataSource = new OBGroupDataSource();

        obGroupDataSource.setUsername("admin");
        obGroupDataSource.setPassword("admin");
        obGroupDataSource.setLMS("10.209.199.244:3307,10.209.199.248:3307");
        obGroupDataSource
            .setConfigURL("http://obconsole.test.alibaba-inc.com/ob-config/config.co?dataId=test_junbian");

        obGroupDataSource.init();

        Connection conn = obGroupDataSource.getConnection();
        java.sql.Statement pstmt = conn.createStatement();

        ResultSet rs = pstmt.executeQuery("select * from __all_server");
        while (rs.next()) {
            rs.getString(1);
        }

        TimeUnit.SECONDS.sleep(70L);
    }
}
