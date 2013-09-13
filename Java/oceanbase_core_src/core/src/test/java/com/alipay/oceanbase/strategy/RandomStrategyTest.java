package com.alipay.oceanbase.strategy;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.alibaba.druid.pool.DruidDataSource;
import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.factory.DataSourceFactory;
import com.alipay.oceanbase.factory.DataSourceHolder;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: RandomStrategyTest.java, v 0.1 Jun 19, 2013 10:32:02 AM liangjie.li Exp $
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DataSourceFactory.class)
public class RandomStrategyTest {

    /**
     * 验证随机负载是否均衡
     * 
     * @throws SQLException 
     */
    @Test
    public void testSelect1() throws SQLException {
        when(mockdsh1.isInvalid()).thenReturn(false);
        when(mockdsh2.isInvalid()).thenReturn(false);
        when(mockdsh3.isInvalid()).thenReturn(false);

        List<DataSourceHolder> excludeList = new ArrayList<DataSourceHolder>();

        int count1 = 0, count2 = 0, count3 = 0, total = 10000;
        for (int i = 0; i < total; i++) {
            DataSourceHolder dsh = mockrs.select(excludeList);
            if (dsh.equals(mockdsh1)) {
                count1++;
            } else if (dsh.equals(mockdsh2)) {
                count2++;
            } else if (dsh.equals(mockdsh3)) {
                count3++;
            }
        }

        assertTrue((total / 4) < count1 && count1 < (total / 2));
        assertTrue((total / 4) < count2 && count2 < (total / 2));
        assertTrue((total / 4) < count3 && count3 < (total / 2));
    }

    /**
     * 当整个集群的merge server不可用时，返回第一次随机结果
     * 
     * @throws SQLException
     */
    @Test
    public void testSelect2_1() throws SQLException {
        when(mockdsh1.isInvalid()).thenReturn(true);
        when(mockdsh2.isInvalid()).thenReturn(true);
        when(mockdsh3.isInvalid()).thenReturn(true);

        List<DataSourceHolder> excludeList = new ArrayList<DataSourceHolder>();

        assertNotNull(mockrs.select(excludeList));
    }

    /**
     * 当整个集群的merge server不可用时，返回第一次随机结果
     * 
     * @throws SQLException
     */
    @Test
    public void testSelect2_2() throws SQLException {
        List<DataSourceHolder> excludeList = new ArrayList<DataSourceHolder>();
        excludeList.add(mockdsh1);
        excludeList.add(mockdsh2);
        excludeList.add(mockdsh3);

        assertNotNull(mockrs.select(excludeList));
    }

    @Test
    public void testIsInvalid() {
        assertFalse(mockrs.isInvalid());
        when(mockdsh1.isInvalid()).thenReturn(true);
        assertFalse(mockrs.isInvalid());
        when(mockdsh2.isInvalid()).thenReturn(true);
        assertFalse(mockrs.isInvalid());
        when(mockdsh3.isInvalid()).thenReturn(true);
        assertTrue(mockrs.isInvalid());
    }

    @Test
    public void testDestroyDataSource1() {
        mockrs.destroyDataSource();

        verify(mockdsh1, times(1)).destroy();
        verify(mockdsh2, times(1)).destroy();
        verify(mockdsh3, times(1)).destroy();
    }

    @Test
    public void testDestroyDataSource2() {
        when(mockmsc1.toString()).thenReturn("10.1.1.1:2828");
        when(mockdsh1.toString()).thenReturn("10.1.1.1:2828");
        mockrs.destroyDataSource(mockmsc1);
        verify(mockdsh1, times(1)).destroy();
    }

    @Test
    public void testAddDataSource() throws SQLException {
        mockrs.addDataSource(mockmsc4);
        mockrs.destroyDataSource();
        verify(mockdsh4, times(1)).destroy();
    }

    @Test
    public void testPrintDSStatus() {
        mockrs.printDSStatus();
    }

    Set<MergeServerConfig> mockMergeServerConfigs;
    Map<String, String>    mockParams;
    MergeServerConfig      mockmsc1, mockmsc2, mockmsc3, mockmsc4;
    DataSourceHolder       mockdsh1, mockdsh2, mockdsh3, mockdsh4;
    RandomStrategy         mockrs;
    DruidDataSource        mockDataSource;

    @Before
    public void setUp() throws Exception {
        mockDataSource = Mockito.mock(DruidDataSource.class);
        when(mockDataSource.getName()).thenReturn("test");
        when(mockDataSource.getCreateCount()).thenReturn(100L);
        when(mockDataSource.getDestroyCount()).thenReturn(100L);
        when(mockDataSource.getCreateErrorCount()).thenReturn(100L);
        when(mockDataSource.getConnectCount()).thenReturn(100L);
        when(mockDataSource.getConnectErrorCount()).thenReturn(100L);
        when(mockDataSource.getCloseCount()).thenReturn(100L);
        when(mockDataSource.getActiveCount()).thenReturn(100);
        when(mockDataSource.getActivePeak()).thenReturn(100);
        when(mockDataSource.getPoolingCount()).thenReturn(100);
        when(mockDataSource.getLockQueueLength()).thenReturn(100);
        when(mockDataSource.getWaitThreadCount()).thenReturn(100);
        when(mockDataSource.getInitialSize()).thenReturn(100);
        when(mockDataSource.getMaxActive()).thenReturn(100);
        when(mockDataSource.getStartTransactionCount()).thenReturn(100L);
        when(mockDataSource.getCommitCount()).thenReturn(100L);
        when(mockDataSource.getRollbackCount()).thenReturn(100L);
        when(mockDataSource.getErrorCount()).thenReturn(100L);
        when(mockDataSource.getMinIdle()).thenReturn(100);
        when(mockDataSource.getCachedPreparedStatementHitCount()).thenReturn(100L);
        when(mockDataSource.getCachedPreparedStatementHitCount()).thenReturn(100L);
        when(mockDataSource.getCachedPreparedStatementMissCount()).thenReturn(100L);

        mockMergeServerConfigs = new LinkedHashSet<MergeServerConfig>();

        mockmsc1 = mock(MergeServerConfig.class);
        mockmsc2 = mock(MergeServerConfig.class);
        mockmsc3 = mock(MergeServerConfig.class);
        mockmsc4 = mock(MergeServerConfig.class);

        mockMergeServerConfigs.add(mockmsc1);
        mockMergeServerConfigs.add(mockmsc2);
        mockMergeServerConfigs.add(mockmsc3);

        mockdsh1 = mock(DataSourceHolder.class);
        when(mockdsh1.getDataSource()).thenReturn(mockDataSource);
        mockdsh2 = mock(DataSourceHolder.class);
        when(mockdsh2.getDataSource()).thenReturn(mockDataSource);
        mockdsh3 = mock(DataSourceHolder.class);
        when(mockdsh3.getDataSource()).thenReturn(mockDataSource);
        mockdsh4 = mock(DataSourceHolder.class);
        when(mockdsh4.getDataSource()).thenReturn(mockDataSource);

        // mock static getHolder method
        spy(DataSourceFactory.class);

        doReturn(mockdsh1).when(DataSourceFactory.class, "getHolder", mockmsc1, mockParams);
        doReturn(mockdsh2).when(DataSourceFactory.class, "getHolder", mockmsc2, mockParams);
        doReturn(mockdsh3).when(DataSourceFactory.class, "getHolder", mockmsc3, mockParams);
        doReturn(mockdsh4).when(DataSourceFactory.class, "getHolder", mockmsc4, mockParams);

        mockrs = new RandomStrategy(mockMergeServerConfigs, mockParams);
    }
}
