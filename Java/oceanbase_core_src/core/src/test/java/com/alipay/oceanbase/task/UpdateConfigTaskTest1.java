package com.alipay.oceanbase.task;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.alipay.oceanbase.OBGroupDataSource;
import com.alipay.oceanbase.config.ClusterConfig;
import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.config.OBDataSourceConfig;
import com.alipay.oceanbase.group.EquityMSManager;
import com.alipay.oceanbase.group.MergeServerSelector;
import com.alipay.oceanbase.strategy.EquityStrategy;
import com.alipay.oceanbase.util.ObUtil;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: UpdateConfigTaskTest.java, v 0.1 Jun 20, 2013 12:26:48 PM liangjie.li Exp $
 */
@RunWith(PowerMockRunner.class)
public class UpdateConfigTaskTest1 {

    @Test
    @PrepareForTest(ObUtil.class)
    public void testCompareServerInfo1() throws Exception {
        mockccs1.add(mockcc1);
        spy(ObUtil.class);
        doReturn(mockccs1).when(ObUtil.class, "getClusterList", mockUserName, mockPassword,
            mockConfigUrl);
        when(mockObDataSourceConfig.getClusterConfigs()).thenReturn(mockccs1);

        mockuct.run();

        verify(mockObGouDataSource, never()).setDBSelector((MergeServerSelector) any());
        verify(mockObGouDataSource, never()).getDBSelector();
        verify(mockObGouDataSource, never()).setConfig((OBDataSourceConfig) any());
        verify(mockObDataSourceConfig, Mockito.never()).destroyAllDruidDS();

        verify(mockes, never()).destroyDataSource((MergeServerConfig) any());
        verify(mockes, never()).addDataSource((MergeServerConfig) any());
    }

    @Test
    @PrepareForTest(ObUtil.class)
    public void testCompareServerInfo2() throws Exception {
        mockccs1.add(mockcc1);
        mockccs2.add(mockcc2);

        spy(ObUtil.class);
        doReturn(mockccs1).when(ObUtil.class, "getClusterList", mockUserName, mockPassword,
            mockConfigUrl);
        when(mockObDataSourceConfig.getClusterConfigs()).thenReturn(mockccs2);

        mockuct.run();

        verify(mockObGouDataSource, times(1)).getDBSelector();
        verify(mockes, times(2)).destroyDataSource((MergeServerConfig) any());
        verify(mockes, times(2)).addDataSource((MergeServerConfig) any());
    }

    EquityStrategy mockes;
    ClusterConfig  mockcc1, mockcc2, mockcc3, mockcc4, mockcc5;
    MergeServerConfig mockmsc1, mockmsc2, mockmsc3, mockmsc4, mockmsc5;

    Set<ClusterConfig> mockccs1, mockccs2, mockccs3, mockccs4;
    Set<MergeServerConfig> mockmscs1, mockmscs2;
    Map<String, String>    mockConfigParams;

    UpdateConfigTask       mockuct;
    String                 mockUserName, mockPassword, mockConfigUrl = "";
    OBDataSourceConfig     mockObDataSourceConfig;
    OBGroupDataSource      mockObGouDataSource;

    @Before
    public void setUp() throws Exception {
        mockObDataSourceConfig = Mockito.mock(OBDataSourceConfig.class);
        mockObGouDataSource = Mockito.mock(OBGroupDataSource.class);
        EquityMSManager equityMSManager = new EquityMSManager(mockObDataSourceConfig, mockuct);
        when(mockObGouDataSource.getDBSelector()).thenReturn(equityMSManager);

        mockuct = Mockito.spy(new UpdateConfigTask(mockUserName, mockPassword, mockConfigUrl,
            mockObDataSourceConfig, mockConfigParams, mockObGouDataSource));
        // Mockito.doReturn(mockConfigUrl).when(mockuct).getClusterAddress();

        mockes = Mockito.mock(EquityStrategy.class);

        mockccs1 = new LinkedHashSet<ClusterConfig>();
        mockccs2 = new LinkedHashSet<ClusterConfig>();
        mockccs3 = new LinkedHashSet<ClusterConfig>();
        mockccs4 = new LinkedHashSet<ClusterConfig>();

        mockmsc1 = new MergeServerConfig("10.1.1.1", 2828L);
        mockmsc2 = new MergeServerConfig("10.1.1.2", 2929L);
        mockmsc3 = new MergeServerConfig("10.1.1.3", 3030L);
        mockmsc4 = new MergeServerConfig("10.1.1.4", 3131L);
        mockmsc5 = new MergeServerConfig("10.1.1.5", 3232L);

        mockmscs1 = new LinkedHashSet<MergeServerConfig>();
        mockmscs1.add(mockmsc1);
        mockmscs1.add(mockmsc2);

        mockmscs2 = new LinkedHashSet<MergeServerConfig>();
        mockmscs2.add(mockmsc3);
        mockmscs2.add(mockmsc4);

        mockcc1 = new ClusterConfig("10.1.1.1", 2828L, 1L, 1L, 50L, 0);
        mockcc1.setEquityStrategy(mockes);
        mockcc1.setServers(mockmscs1);

        mockcc2 = new ClusterConfig("10.1.1.1", 2828L, 1L, 1L, 60L, 0);
        mockcc2.setEquityStrategy(mockes);
        mockcc2.setServers(mockmscs2);

        mockcc3 = new ClusterConfig("10.1.1.2", 2828L, 1L, 1L, 60L, 0);
        mockcc3.setEquityStrategy(mockes);
        mockcc3.setServers(mockmscs2);
    }
}
