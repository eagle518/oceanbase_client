package com.alipay.oceanbase.factory;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.alibaba.druid.pool.DruidDataSource;
import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.util.Helper;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: DataSourceFactoryTest.java, v 0.1 Jun 18, 2013 2:30:06 PM liangjie.li Exp $
 */
@RunWith(PowerMockRunner.class)
public class DataSourceFactoryTest {

    String              mockMsIp;
    long                mockPort;

    MergeServerConfig   mockMergeServerConfig;
    Map<String, String> mockMap1;
    Map<String, String> mockMap2;

    @Test
    @PrepareForTest(DataSourceFactory.class)
    public void testGetHolder() throws Exception {
        // mock druid datasource
        DruidDataSource dds = mock(DruidDataSource.class);

        // mock static newDataSource method
        spy(DataSourceFactory.class);
        doReturn(dds).when(DataSourceFactory.class, "newDataSoruce", mockMergeServerConfig,
            mockMap2);

        // invoke 
        DataSourceHolder dsh = DataSourceFactory.getHolder(mockMergeServerConfig, mockMap2);

        // assert1 
        dsh.destroy();
        verify(dds, times(1)).close();

        // assert2
        DataSource ds = dsh.getDataSource();
        assertEquals(dds, ds);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewDataSoruce1() throws SQLException {
        // blank mockMap
        DataSourceFactory.newDataSoruce(mockMergeServerConfig, mockMap1);
    }

    @Test
    @PrepareForTest({ DataSourceFactory.class, Helper.class })
    public void testNewDataSoruce2() throws Exception {
        // mock druid datasource
        DruidDataSource dds = mock(DruidDataSource.class);

        // mock druid datasource constructor method
        whenNew(DruidDataSource.class).withNoArguments().thenReturn(dds);
        doNothing().when(dds).init();

        // mock Helper.getVersionReportSQL method
        spy(Helper.class);

        doReturn(
            "select /*+ client(obdatasource) client_version(1.1.0) mysql_driver(5.1) */ \'client_version\'")
            .when(Helper.class, "getVersionReportSQL");

        // invoke 
        DruidDataSource _dds = DataSourceFactory.newDataSoruce(mockMergeServerConfig, mockMap2);

        // assert
        assertEquals(dds, _dds);
        verifyNew(DruidDataSource.class).withNoArguments();
        verify(dds, times(1)).init();
    }

    @Before
    public void setUp() {
        mockMsIp = "10.1.1.1";
        mockPort = 2828L;
        mockMergeServerConfig = new MergeServerConfig(mockMsIp, mockPort);

        mockMap1 = new HashMap<String, String>();

        mockMap2 = new HashMap<String, String>();
        mockMap2.put("username", "admin");
        mockMap2.put("password", "admin");
    }

}
