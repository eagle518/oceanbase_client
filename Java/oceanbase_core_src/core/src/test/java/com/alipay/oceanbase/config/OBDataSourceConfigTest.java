package com.alipay.oceanbase.config;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.spy;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.alipay.oceanbase.factory.DataSourceFactory;
import com.alipay.oceanbase.factory.DataSourceHolder;
import com.alipay.oceanbase.util.ObUtil;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: OBDataSourceConfigTest.java, v 0.1 Jun 17, 2013 4:41:58 PM liangjie.li Exp $
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ObUtil.class, DataSourceFactory.class })
public class OBDataSourceConfigTest {

    @Test
    public void testDestroyAllDruidDS() {
        mockObDataSourceConfig.destroyAllDruidDS();

        verify(mockdsh1, times(1)).destroy();
        verify(mockdsh2, times(1)).destroy();
        verify(mockdsh3, times(1)).destroy();
        verify(mockdsh4, times(1)).destroy();
        verify(mockdsh5, times(1)).destroy();
        verify(mockdsh6, times(1)).destroy();
        verify(mockdsh7, times(1)).destroy();
    }

    @Test
    public void testGetConfigParams() {
        assertEquals(mockConfigParams, mockObDataSourceConfig.getConfigParams());
    }

    @Test
    public void testGetClusterConfigs() {
        assertEquals(mockClusterConfigs, mockObDataSourceConfig.getClusterConfigs());
    }

    String                 mockUserName, mockPassword, mockClusterAddress;
    long                   mockClusterId1, mockClusterId2;

    Set<ClusterConfig>     mockClusterConfigs;
    Set<MergeServerConfig> mockMasterMergeServerConfigs;
    Set<MergeServerConfig> mockSlaveMergeServerConfigs;

    Map<String, String>    mockConfigParams;

    OBDataSourceConfig     mockObDataSourceConfig;

    DataSourceHolder       mockdsh1, mockdsh2, mockdsh3, mockdsh4, mockdsh5, mockdsh6, mockdsh7;

    @Before
    public void setUp() throws Exception {
        mockClusterConfigs = new LinkedHashSet<ClusterConfig>();
        ClusterConfig cc1 = new ClusterConfig("10.1.1.1", 2828L, 1L, 1L, 50, 0L);
        ClusterConfig cc2 = new ClusterConfig("10.1.1.2", 2828L, 2L, 2L, 50, 1L);
        mockClusterConfigs.add(cc1);
        mockClusterConfigs.add(cc2);

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
    }

}
