package com.alipay.oceanbase.task;

import static com.alipay.oceanbase.task.UpdateConfigTask.compareAndChangeMergeServer;
import static com.alipay.oceanbase.task.UpdateConfigTask.compareCluster;
import static com.alipay.oceanbase.task.UpdateConfigTask.comparePercent;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.alipay.oceanbase.OBGroupDataSource;
import com.alipay.oceanbase.config.ClusterConfig;
import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.config.OBDataSourceConfig;
import com.alipay.oceanbase.strategy.EquityStrategy;
import com.alipay.oceanbase.util.ObUtil;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: UpdateConfigTaskTest.java, v 0.1 Jun 20, 2013 12:26:48 PM liangjie.li Exp $
 */
@RunWith(PowerMockRunner.class)
public class UpdateConfigTaskTest2 {

    @Test
    @PrepareForTest(ObUtil.class)
    public void testCompareServerInfo1() throws Exception {
        PowerMockito.spy(ObUtil.class);
        PowerMockito.doReturn(mockccs1).when(ObUtil.class, "getClusterList", mockUserName,
            mockPassword, mockConfigUrl);
        //        mockccs1.add(mockcc1);
        //        when(mockObDataSourceConfig.getClusterConfigs()).thenReturn(mockccs1);

        //        mockuct.compareServerInfo();

        //        Mockito.verify(mockObGouDataSource, Mockito.never()).setDBSelector(
        //            (MergeServerSelector) Mockito.any());
        //        Mockito.verify(mockObGouDataSource, Mockito.never()).getDBSelector();
        //        Mockito.verify(mockObGouDataSource, Mockito.never()).setConfig(
        //            (OBDataSourceConfig) Mockito.any());
        //        Mockito.verify(mockObDataSourceConfig, Mockito.never()).destroyAllDruidDS();
        //
        //        Mockito.verify(mockes, Mockito.never())
        //            .destroyDataSource((MergeServerConfig) Mockito.any());
        //        Mockito.verify(mockes, Mockito.never()).addDataSource((MergeServerConfig) Mockito.any());
    }

    @Test
    public void testCompareServerInfo2() {

    }

    /*@Test
    public void testGetClusterAddress1() throws FileNotFoundException, IOException {
        mockConfigUrl = "http://obconsole.test.alibaba-inc.com/ob-config/config.co?dataId=alipay_junbian";
        assertEquals("10.209.144.30:2828", mockuct.getClusterAddress());
    }

    @Test
    public void testGetClusterAddress2() throws SecurityException, NoSuchFieldException, Exception {
        TestUtil.setFinalStatic(UpdateConfigTask.class.getDeclaredField("LOCAL_JAR_PATH"),
            mockLocalConf);
        assertEquals("10.209.144.30:2828", mockuct.getClusterAddress());
    }*/

    @Test
    public void testCompareCluster1() {
        mockccs1.add(mockcc1);
        mockccs2.add(mockcc2);
        mockccs3.add(mockcc3);
        mockccs4.add(mockcc4);
        // one cluster
        assertFalse(compareCluster(mockccs1, mockccs1));
        assertTrue(compareCluster(mockccs1, mockccs2));
        assertTrue(compareCluster(mockccs1, mockccs3));
        assertTrue(compareCluster(mockccs1, mockccs4));
    }

    @Test
    public void testCompareCluster2() {
        mockccs1.add(mockcc1);
        mockccs1.add(mockcc2);
        mockccs2.add(mockcc1);
        mockccs2.add(mockcc3);
        mockccs3.add(mockcc3);
        mockccs3.add(mockcc4);
        // two cluster
        assertFalse(compareCluster(mockccs1, mockccs1));
        assertTrue(compareCluster(mockccs1, mockccs2));
        assertTrue(compareCluster(mockccs1, mockccs3));
    }

    @Test
    public void testCompareCluster3() {
        mockccs1.add(mockcc1);
        mockccs1.add(mockcc2);
        mockccs1.add(mockcc3);
        mockccs2.add(mockcc1);
        mockccs2.add(mockcc2);
        mockccs2.add(mockcc4);
        // three cluster
        assertFalse(compareCluster(mockccs1, mockccs1));
        assertTrue(compareCluster(mockccs1, mockccs2));
    }

    @Test
    public void testCompareCluster4() {
        mockccs1.add(mockcc1);
        mockccs2.add(mockcc1);
        mockccs2.add(mockcc2);
        mockccs3.add(mockcc1);
        mockccs3.add(mockcc2);
        mockccs3.add(mockcc4);
        assertTrue(compareCluster(mockccs1, mockccs2));
        assertTrue(compareCluster(mockccs2, mockccs3));
    }

    @Test
    public void testComparePercent() {
        mockccs1.add(mockcc1);
        mockccs2.add(mockcc2);
        mockccs3.add(mockcc5);
        assertTrue(comparePercent(mockccs1, mockccs3));
        assertFalse(comparePercent(mockccs1, mockccs2));
    }

    @Test
    public void testCompareAndChangeMergeServer1() throws SQLException {
        mockmscs1.add(mockmsc1);
        mockmscs1.add(mockmsc2);
        compareAndChangeMergeServer(mockmscs1, mockmscs1, mockcc1);
        Mockito.verify(mockes, Mockito.never()).addDataSource(null);
        Mockito.verify(mockes, Mockito.never()).destroyDataSource(null);
    }

    @Test
    public void testCompareAndChangeMergeServer2() throws SQLException {
        mockmscs1.add(mockmsc1);
        mockmscs1.add(mockmsc2);
        mockmscs2.add(mockmsc1);
        mockmscs2.add(mockmsc3);
        Mockito.doNothing().when(mockes).destroyDataSource(mockmsc3);
        Mockito.doNothing().when(mockes).addDataSource(mockmsc2);

        compareAndChangeMergeServer(mockmscs1, mockmscs2, mockcc1);
        Mockito.verify(mockes, Mockito.times(1)).addDataSource(mockmsc2);
        Mockito.verify(mockes, Mockito.times(1)).destroyDataSource(mockmsc3);
    }

    @Test
    public void testCompareAndChangeMergeServer3() throws SQLException {
        mockmscs1.add(mockmsc1);
        mockmscs1.add(mockmsc2);
        mockmscs1.add(mockmsc3);
        mockmscs2.add(mockmsc1);
        mockmscs2.add(mockmsc2);
        Mockito.doNothing().when(mockes).addDataSource(mockmsc3);

        compareAndChangeMergeServer(mockmscs1, mockmscs2, mockcc1);
        Mockito.verify(mockes, Mockito.times(1)).addDataSource(mockmsc3);
        Mockito.verify(mockes, Mockito.never()).destroyDataSource(null);
    }

    @Test
    public void testCompareAndChangeMergeServer4() throws SQLException {
        mockmscs1.add(mockmsc1);
        mockmscs1.add(mockmsc2);
        mockmscs2.add(mockmsc1);
        mockmscs2.add(mockmsc2);
        mockmscs2.add(mockmsc3);
        Mockito.doNothing().when(mockes).destroyDataSource(mockmsc3);

        compareAndChangeMergeServer(mockmscs1, mockmscs2, mockcc1);
        Mockito.verify(mockes, Mockito.never()).addDataSource(null);
        Mockito.verify(mockes, Mockito.times(1)).destroyDataSource(mockmsc3);
    }

    EquityStrategy mockes;
    ClusterConfig  mockcc1, mockcc2, mockcc3, mockcc4, mockcc5;
    MergeServerConfig mockmsc1, mockmsc2, mockmsc3, mockmsc4, mockmsc5;
    Set<ClusterConfig> mockccs1, mockccs2, mockccs3, mockccs4;
    Set<MergeServerConfig> mockmscs1, mockmscs2;
    Map<String, String>    mockConfigParams;
    UpdateConfigTask       mockuct;
    String                 mockUserName, mockPassword, mockConfigUrl = "";
    String                 mockLocalConf;
    OBDataSourceConfig     mockObDataSourceConfig;
    OBGroupDataSource      mockObGouDataSource;

    @Before
    public void setUp() throws Exception {
        mockLocalConf = this.getClass().getResource("/conf").getFile().toString();
        mockObDataSourceConfig = Mockito.mock(OBDataSourceConfig.class);
        mockObGouDataSource = Mockito.mock(OBGroupDataSource.class);

        mockuct = new UpdateConfigTask(mockUserName, mockPassword, mockConfigUrl,
            mockObDataSourceConfig, mockConfigParams, mockObGouDataSource);

        mockes = Mockito.mock(EquityStrategy.class);

        mockmsc1 = new MergeServerConfig("10.1.1.1", 2828L);
        mockmsc2 = new MergeServerConfig("10.1.1.2", 2929L);
        mockmsc3 = new MergeServerConfig("10.1.1.3", 3030L);
        mockmsc4 = new MergeServerConfig("10.1.1.4", 3131L);
        mockmsc5 = new MergeServerConfig("10.1.1.5", 3232L);

        mockmscs1 = new LinkedHashSet<MergeServerConfig>();
        mockmscs2 = new LinkedHashSet<MergeServerConfig>();

        mockcc1 = new ClusterConfig("10.1.1.1", 2828L, 1L, 1L, 50L, 0);
        mockcc1.setEquityStrategy(mockes);
        mockcc2 = new ClusterConfig("10.1.1.2", 2828L, 1L, 1L, 50L, 0);
        mockcc3 = new ClusterConfig("10.1.1.1", 2829L, 1L, 1L, 50L, 0);
        mockcc4 = new ClusterConfig("10.1.1.1", 2828L, 1L, 2L, 50L, 0);
        mockcc5 = new ClusterConfig("10.1.1.1", 2828L, 1L, 1L, 60L, 0);

        mockccs1 = new LinkedHashSet<ClusterConfig>();
        mockccs2 = new LinkedHashSet<ClusterConfig>();
        mockccs3 = new LinkedHashSet<ClusterConfig>();
        mockccs4 = new LinkedHashSet<ClusterConfig>();
    }
}
