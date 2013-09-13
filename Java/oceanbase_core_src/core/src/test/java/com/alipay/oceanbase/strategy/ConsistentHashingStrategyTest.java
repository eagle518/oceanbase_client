package com.alipay.oceanbase.strategy;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
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
import com.alipay.oceanbase.jdbc.parameter.ParameterContext;
import com.alipay.oceanbase.jdbc.parameter.ParameterMethod;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: EquityStrategyTest.java, v 0.1 Jun 19, 2013 10:26:34 AM liangjie.li Exp $
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DataSourceFactory.class)
public class ConsistentHashingStrategyTest {

    /**
     * 一致性负载策略是否正确
     */
    @Test
    public void testSelect1() {
        when(mockdsh1.isInvalid()).thenReturn(false);
        when(mockdsh2.isInvalid()).thenReturn(false);
        when(mockdsh3.isInvalid()).thenReturn(false);

        List<DataSourceHolder> excludeList = new ArrayList<DataSourceHolder>();
        for (int i = 0; i < 10000; i++) {
            assertEquals(mockdsh3, mockchs.select(excludeList, mockSql, mockParameterSettings));
        }
    }

    /**
     * 一致性負載策略：當選擇的某server剛好失效，則重選一次，僅只重選一次
     */
    @Test
    public void testSelect2_1() {
        when(mockdsh1.isInvalid()).thenReturn(false);
        when(mockdsh2.isInvalid()).thenReturn(false);
        when(mockdsh3.isInvalid()).thenReturn(true);

        List<DataSourceHolder> excludeList = new ArrayList<DataSourceHolder>();
        for (int i = 0; i < 10000; i++) {
            assertEquals(mockdsh1, mockchs.select(excludeList, mockSql, mockParameterSettings));
        }
    }

    /**
     * 一致性負載策略：當選擇的某server剛好失效，則重選一次，僅只重選一次
     */
    @Test
    public void testSelect2_2() {
        when(mockdsh1.isInvalid()).thenReturn(false);
        when(mockdsh2.isInvalid()).thenReturn(false);
        when(mockdsh3.isInvalid()).thenReturn(false);

        List<DataSourceHolder> excludeList = new ArrayList<DataSourceHolder>();
        excludeList.add(mockdsh3);

        for (int i = 0; i < 10000; i++) {
            assertEquals(mockdsh1, mockchs.select(excludeList, mockSql, mockParameterSettings));
        }
    }

    /**
     * 一致性負載策略：當選擇的某server剛好失效，則重選一次，僅只重選一次
     */
    @Test
    public void testSelect3_1() {
        when(mockdsh1.isInvalid()).thenReturn(true);
        when(mockdsh2.isInvalid()).thenReturn(false);
        when(mockdsh3.isInvalid()).thenReturn(true);

        List<DataSourceHolder> excludeList = new ArrayList<DataSourceHolder>();

        for (int i = 0; i < 10000; i++) {
            assertEquals(mockdsh1, mockchs.select(excludeList, mockSql, mockParameterSettings));
        }
    }

    /**
     * 一致性負載策略：當選擇的某server剛好失效，則重選一次，僅只重選一次
     */
    @Test
    public void testSelect3_2() {
        when(mockdsh1.isInvalid()).thenReturn(false);
        when(mockdsh2.isInvalid()).thenReturn(false);
        when(mockdsh3.isInvalid()).thenReturn(false);

        List<DataSourceHolder> excludeList = new ArrayList<DataSourceHolder>();
        excludeList.add(mockdsh3);
        excludeList.add(mockdsh1);

        for (int i = 0; i < 10000; i++) {
            assertEquals(mockdsh1, mockchs.select(excludeList, mockSql, mockParameterSettings));
        }
    }

    /**
     * 一致性負載策略：當選擇的某server剛好失效，則重選一次，僅只重選一次
     */
    @Test
    public void testSelect4_1() {
        when(mockdsh1.isInvalid()).thenReturn(true);
        when(mockdsh2.isInvalid()).thenReturn(true);
        when(mockdsh3.isInvalid()).thenReturn(true);

        List<DataSourceHolder> excludeList = new ArrayList<DataSourceHolder>();
        for (int i = 0; i < 10000; i++) {
            assertEquals(mockdsh1, mockchs.select(excludeList, mockSql, mockParameterSettings));
        }
    }

    /**
     * 一致性負載策略：當選擇的某server剛好失效，則重選一次，僅只重選一次
     */
    @Test
    public void testSelect4_2() {
        when(mockdsh1.isInvalid()).thenReturn(false);
        when(mockdsh2.isInvalid()).thenReturn(false);
        when(mockdsh3.isInvalid()).thenReturn(false);

        List<DataSourceHolder> excludeList = new ArrayList<DataSourceHolder>();
        excludeList.add(mockdsh3);
        excludeList.add(mockdsh1);
        excludeList.add(mockdsh2);

        for (int i = 0; i < 10000; i++) {
            assertEquals(mockdsh1, mockchs.select(excludeList, mockSql, mockParameterSettings));
        }
    }

    @Test
    public void testIsInvalid() {
        assertFalse(mockchs.isInvalid());
        when(mockdsh1.isInvalid()).thenReturn(true);
        assertFalse(mockchs.isInvalid());
        when(mockdsh2.isInvalid()).thenReturn(true);
        assertFalse(mockchs.isInvalid());
        when(mockdsh3.isInvalid()).thenReturn(true);
        assertTrue(mockchs.isInvalid());
    }

    @Test
    public void testRestorePreparedStatementSQL1() {
        String sql = "select * from tab";
        assertEquals(sql, mockchs.restorePreparedStatementSQL(sql));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRestorePreparedStatementSQL2() {
        mockchs.restorePreparedStatementSQL();
    }

    @Test
    public void testRestorePreparedStatementSQL3() {
        assertEquals(mockSql + mockParameterSettings.get(1),
            mockchs.restorePreparedStatementSQL(mockSql, mockParameterSettings));
    }

    @Test
    public void testAddDataSource() throws SQLException {
        mockchs.addDataSource(mockmsc4);
        mockchs.destroyDataSource();
        verify(mockdsh4, times(1)).destroy();
    }

    @Test
    public void testDestroyDataSource1() {
        mockchs.destroyDataSource();

        verify(mockdsh1, times(1)).destroy();
        verify(mockdsh2, times(1)).destroy();
        verify(mockdsh3, times(1)).destroy();
    }

    @Test
    public void testDestroyDataSource2() {
        mockchs.destroyDataSource(mockmsc1);
        verify(mockdsh1, times(1)).destroy();
    }

    @Test
    public void testPrintDSStatus() {
        mockchs.printDSStatus();
    }

    Set<MergeServerConfig>         mockMergeServerConfigs;
    Map<String, String>            mockParams;
    MergeServerConfig              mockmsc1, mockmsc2, mockmsc3, mockmsc4;
    DataSourceHolder               mockdsh1, mockdsh2, mockdsh3, mockdsh4;
    ConsistentHashingStrategy      mockchs;
    DruidDataSource                mockDataSource;
    String                         mockSql;
    Map<Integer, ParameterContext> mockParameterSettings;

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
        when(mockmsc1.toString()).thenReturn("10.1.1.1:2828");
        mockmsc2 = mock(MergeServerConfig.class);
        when(mockmsc2.toString()).thenReturn("10.1.1.2:2828");
        mockmsc3 = mock(MergeServerConfig.class);
        when(mockmsc3.toString()).thenReturn("10.1.1.3:2828");

        mockmsc4 = mock(MergeServerConfig.class);

        mockMergeServerConfigs.add(mockmsc1);
        mockMergeServerConfigs.add(mockmsc2);
        mockMergeServerConfigs.add(mockmsc3);

        mockdsh1 = mock(DataSourceHolder.class);
        when(mockdsh1.toString()).thenReturn("10.1.1.1:2828");
        when(mockdsh1.getDataSource()).thenReturn(mockDataSource);
        mockdsh2 = mock(DataSourceHolder.class);
        when(mockdsh2.toString()).thenReturn("10.1.1.2:2828");
        when(mockdsh2.getDataSource()).thenReturn(mockDataSource);
        mockdsh3 = mock(DataSourceHolder.class);
        when(mockdsh3.toString()).thenReturn("10.1.1.3:2828");
        when(mockdsh3.getDataSource()).thenReturn(mockDataSource);

        mockdsh4 = mock(DataSourceHolder.class);

        // mock static getHolder method
        spy(DataSourceFactory.class);

        doReturn(mockdsh1).when(DataSourceFactory.class, "getHolder", mockmsc1, mockParams);
        doReturn(mockdsh2).when(DataSourceFactory.class, "getHolder", mockmsc2, mockParams);
        doReturn(mockdsh3).when(DataSourceFactory.class, "getHolder", mockmsc3, mockParams);
        doReturn(mockdsh4).when(DataSourceFactory.class, "getHolder", mockmsc4, mockParams);

        mockchs = new ConsistentHashingStrategy(mockMergeServerConfigs, mockParams);

        mockSql = "select * from tab where col1 = ?";
        mockParameterSettings = new HashMap<Integer, ParameterContext>();
        mockParameterSettings.put(1, new ParameterContext(ParameterMethod.setInt, new Object[] { 1,
                1 }));
    }
}
