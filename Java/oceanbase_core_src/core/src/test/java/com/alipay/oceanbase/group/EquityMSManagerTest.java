package com.alipay.oceanbase.group;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.alipay.oceanbase.config.ClusterConfig;
import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.config.OBDataSourceConfig;
import com.alipay.oceanbase.factory.DataSourceFactory;
import com.alipay.oceanbase.factory.DataSourceHolder;
import com.alipay.oceanbase.group.MergeServerSelector.DataSourceTryer;
import com.alipay.oceanbase.task.UpdateConfigTask;
import com.alipay.oceanbase.util.ObUtil;
import com.alipay.oceanbase.util.parse.SqlHintType;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: EquityMSManagerTest.java, v 0.1 Jun 19, 2013 2:52:49 PM liangjie.li Exp $
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ObUtil.class, DataSourceFactory.class })
public class EquityMSManagerTest {

    /**
     * 写操作，选择主集群
     */
    @Test
    public void testSelectCluster1_1() {
        for (int i = 0; i < 10000; i++) {
            assertEquals(mockMasterClusterConfig,
                mockEquityMSManager.selectCluster(true, SqlHintType.CLUSTER_NONE));
        }
    }

    /**
     * 写操作，选择主集群，即使主集群不可用
     */
    @Test
    public void testSelectCluster1_2() {
        when(mockMasterClusterConfig.isInvalid()).thenReturn(true);
        for (int i = 0; i < 10000; i++) {
            assertEquals(mockMasterClusterConfig,
                mockEquityMSManager.selectCluster(true, SqlHintType.CLUSTER_NONE));
        }
    }

    /**
     * 读操作，主备按照流量分配选择
     */
    @Test
    public void testSelectCluster2_1() {
        int count1 = 0, count2 = 0;
        for (int i = 0; i < 10000; i++) {
            ClusterConfig cc = mockEquityMSManager.selectCluster(false, SqlHintType.CLUSTER_NONE);
            if (cc.equals(mockMasterClusterConfig)) {
                count1++;
            } else if (cc.equals(mockSlaveClusterConfig)) {
                count2++;
            }
        }
        assertTrue(count1 > 4900 && count1 < 5100);
        assertTrue(count2 > 4900 && count2 < 5100);
    }

    /**
     * 读操作，主备按照流量分配选择。如果主集群不可用，则选择备
     */
    @Test
    public void testSelectCluster2_2() {
        doReturn(true).when(mockMasterClusterConfig).isInvalid();
        int count1 = 0, count2 = 0;
        for (int i = 0; i < 10000; i++) {
            ClusterConfig cc = mockEquityMSManager.selectCluster(false, SqlHintType.CLUSTER_NONE);
            if (cc.equals(mockMasterClusterConfig)) {
                count1++;
            } else if (cc.equals(mockSlaveClusterConfig)) {
                count2++;
            }
        }
        assertEquals(0, count1);
        assertEquals(10000, count2);
    }

    /**
     * 读操作，主备按照流量分配选择。如果主备集群都不可用
     */
    @Test
    public void testSelectCluster2_3() {
        doReturn(true).when(mockMasterClusterConfig).isInvalid();
        doReturn(true).when(mockSlaveClusterConfig).isInvalid();
        int count1 = 0, count2 = 0;
        for (int i = 0; i < 10000; i++) {
            ClusterConfig cc = mockEquityMSManager.selectCluster(false, SqlHintType.CLUSTER_NONE);
            if (cc.equals(mockMasterClusterConfig)) {
                count1++;
            } else if (cc.equals(mockSlaveClusterConfig)) {
                count2++;
            }
        }
        assertTrue(count1 == 0 || count1 == 10000);
        assertTrue(count2 == 0 || count2 == 10000);
    }

    /**
     * 读写操作同时进行
     */
    @Test
    public void testSelectCluster3_1() {
        int count1 = 0, count2 = 0, count3 = 0;
        for (int i = 0; i < 10000; i++) {
            ClusterConfig cc = mockEquityMSManager.selectCluster(true, SqlHintType.CLUSTER_NONE);
            ClusterConfig _cc = mockEquityMSManager.selectCluster(false, SqlHintType.CLUSTER_NONE);
            if (cc.equals(mockMasterClusterConfig)) {
                count1++;
            }
            if (_cc.equals(mockMasterClusterConfig)) {
                count2++;
            } else if (_cc.equals(mockSlaveClusterConfig)) {
                count3++;
            }
        }

        assertTrue(count1 == 10000);
        assertTrue(count2 < 100);
        assertTrue(count3 > 9900);
    }

    /**
     * 读写操作同时进行, 主集群不可用
     */
    @Test
    public void testSelectCluster3_2() {
        doReturn(true).when(mockMasterClusterConfig).isInvalid();

        int count1 = 0, count2 = 0, count3 = 0;
        for (int i = 0; i < 10000; i++) {
            ClusterConfig cc = mockEquityMSManager.selectCluster(true, SqlHintType.CLUSTER_NONE);
            ClusterConfig _cc = mockEquityMSManager.selectCluster(false, SqlHintType.CLUSTER_NONE);
            if (cc.equals(mockMasterClusterConfig)) {
                count1++;
            }
            if (_cc.equals(mockMasterClusterConfig)) {
                count2++;
            } else if (_cc.equals(mockSlaveClusterConfig)) {
                count3++;
            }
        }

        assertTrue(count1 == 10000);
        assertTrue(count2 < 10);
        assertTrue(count3 > 9990);
    }

    @Test
    public void testTryExecute1_1() throws SQLException {
        when(mockDataSourceTryer.tryOnDataSource(mockdsh1, mockSQL)).thenReturn(100);
        when(mockDataSourceTryer.tryOnDataSource(mockdsh2, mockSQL)).thenReturn(100);
        when(mockDataSourceTryer.tryOnDataSource(mockdsh3, mockSQL)).thenReturn(100);

        assertTrue(mockEquityMSManager.tryExecute(mockDataSourceTryer, true,
            SqlHintType.CLUSTER_NONE, mockSQL).compareTo(100) == 0);
    }

    @Test(expected = SQLException.class)
    public void testTryExecute1_2() throws SQLException {
        when(mockDataSourceTryer.tryOnDataSource(mockdsh1, mockSQL)).thenThrow(new SQLException());
        when(mockDataSourceTryer.tryOnDataSource(mockdsh2, mockSQL)).thenThrow(new SQLException());
        when(mockDataSourceTryer.tryOnDataSource(mockdsh3, mockSQL)).thenThrow(new SQLException());

        assertTrue(mockEquityMSManager.tryExecute(mockDataSourceTryer, true,
            SqlHintType.CLUSTER_NONE, mockSQL).compareTo(100) == 0);
    }

    @Test
    public void testTryExecute1_3() throws SQLException {
        when(mockDataSourceTryer.tryOnDataSource(mockdsh1, mockSQL)).thenThrow(new SQLException());
        when(mockDataSourceTryer.tryOnDataSource(mockdsh2, mockSQL)).thenReturn(100);
        when(mockDataSourceTryer.tryOnDataSource(mockdsh3, mockSQL)).thenReturn(100);

        assertTrue(mockEquityMSManager.tryExecute(mockDataSourceTryer, true,
            SqlHintType.CLUSTER_NONE, mockSQL).compareTo(100) == 0);
    }

    @Test(expected = SQLException.class)
    public void testTryExecute2_1() throws SQLException {
        when(mockDataSourceTryer.tryOnDataSource(mockdsh1, mockSQL)).thenThrow(new SQLException());
        when(mockDataSourceTryer.tryOnDataSource(mockdsh2, mockSQL)).thenThrow(new SQLException());
        when(mockDataSourceTryer.tryOnDataSource(mockdsh3, mockSQL)).thenThrow(new SQLException());
        when(mockDataSourceTryer.tryOnDataSource(mockdsh4, mockSQL)).thenThrow(new SQLException());
        when(mockDataSourceTryer.tryOnDataSource(mockdsh5, mockSQL)).thenThrow(new SQLException());
        when(mockDataSourceTryer.tryOnDataSource(mockdsh6, mockSQL)).thenThrow(new SQLException());
        when(mockDataSourceTryer.tryOnDataSource(mockdsh7, mockSQL)).thenThrow(new SQLException());

        assertTrue(mockEquityMSManager.tryExecute(mockDataSourceTryer, false,
            SqlHintType.CLUSTER_NONE, mockSQL).compareTo(100) == 0);
    }

    @Test
    public void testTryExecute2_2() throws SQLException {
        when(mockDataSourceTryer.tryOnDataSource(mockdsh1, mockSQL)).thenReturn(100);
        when(mockDataSourceTryer.tryOnDataSource(mockdsh2, mockSQL)).thenReturn(100);
        when(mockDataSourceTryer.tryOnDataSource(mockdsh3, mockSQL)).thenReturn(100);
        when(mockDataSourceTryer.tryOnDataSource(mockdsh4, mockSQL)).thenReturn(100);
        when(mockDataSourceTryer.tryOnDataSource(mockdsh5, mockSQL)).thenReturn(100);
        when(mockDataSourceTryer.tryOnDataSource(mockdsh6, mockSQL)).thenReturn(100);
        when(mockDataSourceTryer.tryOnDataSource(mockdsh7, mockSQL)).thenReturn(100);

        assertTrue(mockEquityMSManager.tryExecute(mockDataSourceTryer, false,
            SqlHintType.CLUSTER_NONE, mockSQL).compareTo(100) == 0);
    }

    String                   mockUserName, mockPassword, mockClusterAddress, mockSQL;
    long                     mockClusterId1, mockClusterId2;
    ClusterConfig            mockMasterClusterConfig, mockSlaveClusterConfig;
    Set<ClusterConfig>       mockClusterConfigs;
    Set<MergeServerConfig>   mockMasterMergeServerConfigs;
    Set<MergeServerConfig>   mockSlaveMergeServerConfigs;
    Map<String, String>      mockConfigParams;
    OBDataSourceConfig       mockObDataSourceConfig;
    DataSourceHolder         mockdsh1, mockdsh2, mockdsh3, mockdsh4, mockdsh5, mockdsh6, mockdsh7;
    EquityMSManager          mockEquityMSManager;
    UpdateConfigTask         mockUpdateConfigTask;

    DataSourceTryer<Integer> mockDataSourceTryer;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        mockClusterConfigs = new LinkedHashSet<ClusterConfig>();

        mockMasterClusterConfig = Mockito.spy(new ClusterConfig("10.1.1.1", 2828L, 1L, 1L, 50, 0L));
        mockSlaveClusterConfig = Mockito.spy(new ClusterConfig("10.1.1.2", 2828L, 2L, 2L, 50, 1L));
        mockClusterConfigs.add(mockMasterClusterConfig);
        mockClusterConfigs.add(mockSlaveClusterConfig);

        mockMasterMergeServerConfigs = new LinkedHashSet<MergeServerConfig>();
        MergeServerConfig msc1 = new MergeServerConfig("10.2.1.1", 40986L);
        MergeServerConfig msc2 = new MergeServerConfig("10.2.1.2", 40986L);
        MergeServerConfig msc3 = new MergeServerConfig("10.2.1.3", 40986L);
        mockMasterMergeServerConfigs.add(msc1);
        mockMasterMergeServerConfigs.add(msc2);
        mockMasterMergeServerConfigs.add(msc3);

        mockSlaveMergeServerConfigs = new LinkedHashSet<MergeServerConfig>();
        MergeServerConfig _msc1 = new MergeServerConfig("10.2.0.1", 40986L);
        MergeServerConfig _msc2 = new MergeServerConfig("10.2.0.2", 40986L);
        MergeServerConfig _msc3 = new MergeServerConfig("10.2.0.3", 40986L);
        MergeServerConfig _msc4 = new MergeServerConfig("10.2.0.4", 40986L);
        mockSlaveMergeServerConfigs.add(_msc1);
        mockSlaveMergeServerConfigs.add(_msc2);
        mockSlaveMergeServerConfigs.add(_msc3);
        mockSlaveMergeServerConfigs.add(_msc4);

        mockdsh1 = mock(DataSourceHolder.class);
        mockdsh2 = mock(DataSourceHolder.class);
        mockdsh3 = mock(DataSourceHolder.class);
        mockdsh4 = mock(DataSourceHolder.class);
        mockdsh5 = mock(DataSourceHolder.class);
        mockdsh6 = mock(DataSourceHolder.class);
        mockdsh7 = mock(DataSourceHolder.class);

        spy(ObUtil.class);
        doReturn(mockClusterConfigs).when(ObUtil.class, "getClusterList", mockUserName,
            mockPassword, mockClusterAddress);

        doReturn(mockMasterMergeServerConfigs).when(ObUtil.class, "getServerList", mockUserName,
            mockPassword, mockClusterAddress, 1L);
        doReturn(mockSlaveMergeServerConfigs).when(ObUtil.class, "getServerList", mockUserName,
            mockPassword, mockClusterAddress, 2L);

        spy(DataSourceFactory.class);

        doReturn(mockdsh1).when(DataSourceFactory.class, "getHolder", msc1, mockConfigParams);
        doReturn(mockdsh2).when(DataSourceFactory.class, "getHolder", msc2, mockConfigParams);
        doReturn(mockdsh3).when(DataSourceFactory.class, "getHolder", msc3, mockConfigParams);
        doReturn(mockdsh4).when(DataSourceFactory.class, "getHolder", _msc1, mockConfigParams);
        doReturn(mockdsh5).when(DataSourceFactory.class, "getHolder", _msc2, mockConfigParams);
        doReturn(mockdsh6).when(DataSourceFactory.class, "getHolder", _msc3, mockConfigParams);
        doReturn(mockdsh7).when(DataSourceFactory.class, "getHolder", _msc4, mockConfigParams);

        mockObDataSourceConfig = new OBDataSourceConfig(mockUserName, mockPassword,
            mockClusterAddress, mockConfigParams);

        mockUpdateConfigTask = Mockito.mock(UpdateConfigTask.class);
        Mockito.doNothing().when(mockUpdateConfigTask).run();

        mockEquityMSManager = new EquityMSManager(mockObDataSourceConfig, mockUpdateConfigTask);

        mockDataSourceTryer = mock(DataSourceTryer.class);
        mockSQL = "select * from tab where col = 1";
    }
}
