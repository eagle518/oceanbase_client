package com.alipay.oceanbase.util;

import static com.alipay.oceanbase.util.ObUtil.CLUSTER_INFO;
import static com.alipay.oceanbase.util.ObUtil.READ_CONSISTENCY_LEVEL;
import static com.alipay.oceanbase.util.ObUtil.SERVER_INFO_BY_ID;
import static com.alipay.oceanbase.util.ObUtil.closeConnection;
import static com.alipay.oceanbase.util.ObUtil.closeResultSet;
import static com.alipay.oceanbase.util.ObUtil.closeStatement;
import static com.alipay.oceanbase.util.ObUtil.executeSQL;
import static com.alipay.oceanbase.util.ObUtil.getClusterList;
import static com.alipay.oceanbase.util.ObUtil.getServerList;
import static com.alipay.oceanbase.util.ObUtil.isConsistency;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.spy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.alipay.oceanbase.TGroupConnection;
import com.alipay.oceanbase.TGroupStatement;
import com.alipay.oceanbase.config.ClusterConfig;
import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.strategy.WeakConsistencyStrategy;
import com.mysql.jdbc.ResultSetImpl;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: ObUtilTest.java, v 0.1 Jun 17, 2013 5:22:38 PM liangjie.li Exp $
 */
@RunWith(PowerMockRunner.class)
public class ObUtilTest {

    @Test
    @PrepareForTest(ObUtil.class)
    public void testGetClusterList() throws Exception {
        // mock static method
        spy(ObUtil.class);
        doReturn(mockClusterData).when(ObUtil.class, "executeSQL", userName, password, ip,
            CLUSTER_INFO);

        // invoke 
        Connection conn = ObUtil.getConnection(userName, password, ip);
        Set<ClusterConfig> result = getClusterList(userName, password, conn);
        ObUtil.closeConnection(conn);

        // assert
        assertEquals(2, result.size());
        ClusterConfig cc = result.iterator().next();
        assertEquals(mockMasterData.get("cluster_id"), cc.getClusterid());
        assertEquals(mockMasterData.get("cluster_vip"), cc.getIp());
        assertEquals(mockMasterData.get("cluster_role"), cc.getRole());
        assertEquals(mockMasterData.get("cluster_flow_percent"), cc.getPercent());
        assertEquals(mockMasterData.get("cluster_port"), cc.getPort());
        assertEquals(
            WeakConsistencyStrategy.getStrategy((Long) mockMasterData.get("read_strategy")),
            cc.getReadStrategy());
    }

    @Test
    @PrepareForTest(ObUtil.class)
    public void testGetServerList1() throws Exception {
        // mock static method
        spy(ObUtil.class);
        doReturn(mockServerData1).when(ObUtil.class, "executeSQL", userName, password, ip,
            SERVER_INFO_BY_ID + clusterId);

        // invoke 
        Connection conn = ObUtil.getConnection(userName, password, ip);
        Set<MergeServerConfig> result = getServerList(conn, clusterId);
        ObUtil.closeConnection(conn);

        // assert
        assertEquals(2, result.size());
        Iterator<MergeServerConfig> it = result.iterator();

        MergeServerConfig msc2 = it.next();
        assertEquals(mockMs2.get("svr_ip"), msc2.getIp());
        assertEquals(mockMs2.get("svr_port"), msc2.getPort());

        it.hasNext();
        MergeServerConfig msc3 = it.next();
        assertEquals(mockMs3.get("svr_ip"), msc3.getIp());
        assertEquals(mockMs3.get("svr_port"), msc3.getPort());
    }

    @Test(expected = IllegalArgumentException.class)
    @PrepareForTest(ObUtil.class)
    public void testGetServerList2() throws Exception {
        // mock static method
        spy(ObUtil.class);
        doReturn(mockServerData2).when(ObUtil.class, "executeSQL", userName, password, ip,
            SERVER_INFO_BY_ID + clusterId);

        // invoke 
        Connection conn = ObUtil.getConnection(userName, password, ip);
        getServerList(conn, clusterId);
        ObUtil.closeConnection(conn);
    }

    @Test
    @PrepareForTest(ObUtil.class)
    public void testIsConsistency() throws Exception {
        // mock static method
        spy(ObUtil.class);
        doReturn(mockIsConsistencyData).when(ObUtil.class, "executeSQL", userName, password, ip,
            READ_CONSISTENCY_LEVEL);

        // invoke  && assert
        assertTrue(isConsistency(userName, password, ip) >= 4);
    }

    @Test
    public void testExecuteSQL1() throws SQLException {
        String userName = "admin";
        String password = "admin";
        String ip = "10.209.144.30:2828";

        // invoke
        List<Map<String, Object>> result = executeSQL(userName, password, ip, CLUSTER_INFO);

        // assert
        assertEquals(1, result.size());
        Map<String, Object> resultMap = result.get(0);
        assertTrue(((Long) resultMap.get("cluster_id")).compareTo(0L) == 0);
        assertTrue(((Long) resultMap.get("cluster_role")).compareTo(1L) == 0);
        assertTrue(((Long) resultMap.get("cluster_flow_percent")).compareTo(100L) == 0);
        assertTrue(((Long) resultMap.get("cluster_port")).compareTo(2828L) == 0);
        assertTrue(((Long) resultMap.get("read_strategy")).compareTo(0L) == 0);
        assertEquals("10.209.144.30", (String) resultMap.get("cluster_vip"));
    }

    @Test
    public void testExecuteSQL2() throws SQLException {
        String userName = "admin";
        String password = "admin";
        String ip = "10.1.1.1:2828,10.209.144.30:2828";

        // invoke
        List<Map<String, Object>> result = executeSQL(userName, password, ip, CLUSTER_INFO);

        // assert
        assertEquals(1, result.size());
        Map<String, Object> resultMap = result.get(0);
        assertTrue(((Long) resultMap.get("cluster_id")).compareTo(0L) == 0);
        assertTrue(((Long) resultMap.get("cluster_role")).compareTo(1L) == 0);
        assertTrue(((Long) resultMap.get("cluster_flow_percent")).compareTo(100L) == 0);
        assertTrue(((Long) resultMap.get("cluster_port")).compareTo(2828L) == 0);
        assertTrue(((Long) resultMap.get("read_strategy")).compareTo(0L) == 0);
        assertEquals("10.209.144.30", (String) resultMap.get("cluster_vip"));
    }

    @Test(expected = SQLException.class)
    public void testExecuteSQL3() throws SQLException {
        String userName = "admin";
        String password = "admin";
        String ip = "10.1.1.1:2828,10.1.1.2:2828";

        // invoke
        executeSQL(userName, password, ip, CLUSTER_INFO);
    }

    @Test
    public void testCloseConnection() throws SQLException {
        Connection connection = mock(TGroupConnection.class);
        closeConnection(connection);

        verify(connection, times(1)).close();
    }

    @Test
    public void testCloseStatement() throws SQLException {
        Statement stmt = mock(TGroupStatement.class);
        closeStatement(stmt);

        verify(stmt, times(1)).close();
    }

    @Test
    public void testCloseResultSet() throws SQLException {
        ResultSet rs = mock(ResultSetImpl.class);
        closeResultSet(rs);

        verify(rs, times(1)).close();
    }

    String                    userName, password, ip;
    long                      clusterId;

    List<Map<String, Object>> mockClusterData;
    List<Map<String, Object>> mockServerData1;
    List<Map<String, Object>> mockServerData2;
    List<Map<String, Object>> mockIsConsistencyData;
    Map<String, Object>       mockMasterData;
    Map<String, Object>       mockSlaveData;
    Map<String, Object>       mockConsistencyResult;
    Map<String, Object>       mockMs1;
    Map<String, Object>       mockMs2;
    Map<String, Object>       mockMs3;

    @Before
    public void setUp() {
        // mock cluster data
        mockClusterData = new ArrayList<Map<String, Object>>();
        mockMasterData = new HashMap<String, Object>();
        mockSlaveData = new HashMap<String, Object>();

        mockMasterData.put("cluster_vip", "10.1.1.1");
        mockMasterData.put("cluster_role", 1L);
        mockMasterData.put("cluster_flow_percent", 50L);
        mockMasterData.put("cluster_port", 2828L);
        mockMasterData.put("cluster_id", 1L);
        mockMasterData.put("read_strategy", 1L);
        mockClusterData.add(mockMasterData);

        mockSlaveData.put("cluster_vip", "10.1.1.0");
        mockSlaveData.put("cluster_role", 2L);
        mockSlaveData.put("cluster_flow_percent", 50L);
        mockSlaveData.put("cluster_port", 2828L);
        mockSlaveData.put("cluster_id", 2L);
        mockSlaveData.put("read_strategy", 1L);
        mockClusterData.add(mockSlaveData);

        // mock consistency data
        mockIsConsistencyData = new ArrayList<Map<String, Object>>();
        mockConsistencyResult = new HashMap<String, Object>();

        mockConsistencyResult.put("value", "1");
        mockIsConsistencyData.add(mockConsistencyResult);

        // mock merge server data
        mockServerData1 = new ArrayList<Map<String, Object>>();
        mockServerData2 = new ArrayList<Map<String, Object>>();

        mockMs1 = new HashMap<String, Object>();
        // ignore mockMs1.put("svr_ip", "10.1.1.1");
        mockMs1.put("svr_port", 2828L);
        mockServerData2.add(mockMs1);

        mockMs2 = new HashMap<String, Object>();
        mockMs2.put("svr_ip", "10.1.1.2");
        mockMs2.put("svr_port", 2829L);
        mockServerData1.add(mockMs2);

        mockMs3 = new HashMap<String, Object>();
        mockMs3.put("svr_ip", "10.1.1.3");
        mockMs3.put("svr_port", 2830L);
        mockServerData1.add(mockMs3);
    }

}
