package com.alipay.oceanbase.factory;

import static com.alipay.oceanbase.util.OBDataSourceConstants.AUDIT_THRESHOLD;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.alibaba.druid.pool.DruidDataSource;
import com.alipay.oceanbase.config.MergeServerConfig;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: DataSourceHolderTest.java, v 0.1 Jun 18, 2013 4:44:01 PM liangjie.li Exp $
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DataSourceFactory.class)
public class DataSourceHolderTest {

    String              mockMsIp1;
    String              mockMsIp2;
    long                mockPort;

    MergeServerConfig   mockMergeServerConfig1;
    MergeServerConfig   mockMergeServerConfig2;
    Map<String, String> mockMap;
    DataSourceHolder    mockDataSourceHolder;

    @Test
    public void testAudit1() {
        for (int i = 0; i < AUDIT_THRESHOLD; i++) {
            mockDataSourceHolder.audit(1.0D);
        }
        assertTrue(mockDataSourceHolder.isInvalid());
    }

    @Test
    public void testAudit2() throws InterruptedException {
        for (int i = 0; i < AUDIT_THRESHOLD; i++) {
            mockDataSourceHolder.audit(1.0D);
        }

        TimeUnit.MINUTES.sleep(1L);
        assertFalse(mockDataSourceHolder.isInvalid());
    }

    @Test
    public void testInvalid() throws InterruptedException {
        assertFalse(mockDataSourceHolder.isInvalid());
        for (int i = 0; i < AUDIT_THRESHOLD; i++) {
            mockDataSourceHolder.audit(1.0D);
        }
        assertTrue(mockDataSourceHolder.isInvalid());

        TimeUnit.MINUTES.sleep(1L);
        assertFalse(mockDataSourceHolder.isInvalid());
    }

    @Test
    public void testEquals() throws SQLException {
        assertTrue(mockDataSourceHolder.equals(mockDataSourceHolder));

        assertTrue(mockDataSourceHolder.equals(DataSourceFactory.getHolder(mockMergeServerConfig1,
            mockMap)));

        assertFalse(mockDataSourceHolder.equals(DataSourceFactory.getHolder(mockMergeServerConfig2,
            mockMap)));
    }

    @Test
    public void testToString() {
        assertEquals(mockMergeServerConfig1.toString(), mockDataSourceHolder.toString());
    }

    @Before
    public void setUp() throws Exception {
        mockMsIp1 = "10.1.1.1";
        mockMsIp2 = "10.1.1.2";
        mockPort = 2828L;
        mockMergeServerConfig1 = new MergeServerConfig(mockMsIp1, mockPort);
        mockMergeServerConfig2 = new MergeServerConfig(mockMsIp2, mockPort);

        mockMap = new HashMap<String, String>();
        mockMap.put("username", "admin");
        mockMap.put("password", "admin");

        // mock druid datasource
        DruidDataSource dds = mock(DruidDataSource.class);

        // mock static newDataSource method
        spy(DataSourceFactory.class);
        doReturn(dds).when(DataSourceFactory.class, "newDataSoruce", mockMergeServerConfig1,
            mockMap);

        doReturn(dds).when(DataSourceFactory.class, "newDataSoruce", mockMergeServerConfig2,
            mockMap);

        // invoke 
        mockDataSourceHolder = DataSourceFactory.getHolder(mockMergeServerConfig1, mockMap);
    }
}
