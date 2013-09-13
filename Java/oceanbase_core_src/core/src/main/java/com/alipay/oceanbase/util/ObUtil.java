package com.alipay.oceanbase.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.alipay.oceanbase.config.ClusterConfig;
import com.alipay.oceanbase.config.MergeServerConfig;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: ObUtil.java, v 0.1 2013-4-1 13:20:43 liangjie.li Exp $
 */
public class ObUtil {

    private static final Logger logger                 = Logger.getLogger(ObUtil.class);

    static final String         CLUSTER_INFO           = "select cluster_id,cluster_role,cluster_flow_percent,cluster_vip,cluster_port,read_strategy from __all_cluster";
    static final String         MASTER_CLUSTER_INFO    = "select cluster_vip, cluster_port from __all_cluster where cluster_role = 1";
    static final String         SERVER_INFO_BY_ID      = "select svr_ip, svr_port from __all_server where svr_type = 'mergeserver' and cluster_id=";
    static final String         READ_CONSISTENCY_LEVEL = "show variables like 'ob_read_consistency'";

    static String               LMS_ADDRESS            = null;

    /**
     * 
     * 
     * @param userName
     * @param password
     * @param clusterAddress
     * @param configParams
     * @return
     * @throws SQLException
     */
    public static Set<ClusterConfig> initClusterInfo(String userName, String password,
                                                     Connection conn,
                                                     Map<String, String> configParams)
                                                                                      throws SQLException {
        Set<ClusterConfig> clusterConfigs = ObUtil.getClusterList(userName, password, conn);

        for (ClusterConfig cc : clusterConfigs) {
            // 1. get all mergeserver per cluster
            Set<MergeServerConfig> mergeServerConfigs = ObUtil.getServerList(conn,
                cc.getClusterid());
            cc.setServers(mergeServerConfigs);

            if (logger.isInfoEnabled()) {
                logger.info("cluster info {" + cc + "}");
            }
        }

        return clusterConfigs;
    }

    /**
     * select cluster_id,cluster_role,cluster_flow_percent,cluster_vip,cluster_port,read_strategy from __all_cluster
     * 
     * @param config
     * @param rs
     * @return
     * @throws Exception
     */
    public static Set<ClusterConfig> getClusterList(String userName, String password,
                                                    Connection conn) throws SQLException {

        List<Map<String, Object>> clusters = ObUtil.executeSQL(conn, ObUtil.CLUSTER_INFO);

        for (Map<String, Object> map : clusters) {// update property: LMS_ADDRESS
            String url = (String) map.get("cluster_vip") + ":" + (Long) map.get("cluster_port");

            if (((Long) map.get("cluster_role")).compareTo(1L) == 0 && !url.equals(LMS_ADDRESS)) {
                LMS_ADDRESS = url;
                return fillClusterList(ObUtil.executeSQL(userName, password, LMS_ADDRESS,
                    ObUtil.CLUSTER_INFO));
            } else if (((Long) map.get("cluster_role")).compareTo(1L) == 0
                       && url.equals(LMS_ADDRESS)) {
                break;
            }
        }

        return fillClusterList(clusters);
    }

    public static Set<ClusterConfig> fillClusterList(List<Map<String, Object>> clusters) {
        Set<ClusterConfig> clusterSet = new LinkedHashSet<ClusterConfig>(2);

        for (Map<String, Object> map : clusters) {
            ClusterConfig cc = new ClusterConfig((String) map.get("cluster_vip"),
                (Long) map.get("cluster_port"), (Long) map.get("cluster_id"),
                (Long) map.get("cluster_role"), (Long) map.get("cluster_flow_percent"),
                (Long) map.get("read_strategy"));
            clusterSet.add(cc);
        }
        return clusterSet;
    }

    /**
     * get MergeServer list,
     *   select svr_ip, svr_port from __all_server where svr_type = 'mergeserver' and cluster_id = ?
     * 
     * @param config
     * @param rs
     * @param cluster
     * @throws Exception
     */
    public static Set<MergeServerConfig> getServerList(Connection conn, long clusterId)
                                                                                       throws SQLException {
        Set<MergeServerConfig> mergeServerSet = new LinkedHashSet<MergeServerConfig>();

        List<Map<String, Object>> servers = ObUtil.executeSQL(conn, ObUtil.SERVER_INFO_BY_ID
                                                                    + clusterId);
        for (Map<String, Object> map : servers) {
            MergeServerConfig ms = new MergeServerConfig((String) map.get("svr_ip"),
                (Long) map.get("svr_port"));

            mergeServerSet.add(ms);
        }

        return mergeServerSet;
    }

    /**
     * consistency level
     * 
     * @param config
     * @param server
     * @return
     */
    public static Integer isConsistency(String userName, String password, String ip) {
        try {
            List<Map<String, Object>> servers = ObUtil.executeSQL(userName, password, ip,
                READ_CONSISTENCY_LEVEL);
            for (Map<String, Object> fields : servers) {
                if (logger.isInfoEnabled()) {
                    logger.info("global value -> ob_read_consistency: " + fields.get("value"));
                }

                return Integer.parseInt((String) fields.get("value"));
            }
        } catch (SQLException ex) {
            return -1;
        }
        return -1;
    }

    /**
     * execute sql
     * 
     * @param config
     * @param ip
     * @param sql
     * @return
     * @throws SQLException
     */
    public static List<Map<String, Object>> executeSQL(String userName, String password, String ip,
                                                       String sql) throws SQLException {
        if (StringUtils.isBlank(LMS_ADDRESS)) {
            LMS_ADDRESS = getMasterLMS(userName, password, ip);
        }

        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = DriverManager.getConnection("jdbc:mysql://" + LMS_ADDRESS, userName, password);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);

            ResultSetMetaData medaData = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> map = new LinkedHashMap<String, Object>();
                for (int i = 1; i <= medaData.getColumnCount(); i++) {
                    map.put(medaData.getColumnName(i), rs.getObject(i));
                }
                list.add(map);
            }
        } catch (SQLException sqlException) {
            throw sqlException;
        } finally {
            closeResultSet(rs);
            closeStatement(stmt);
            closeConnection(conn);
        }

        return list;
    }

    public static List<Map<String, Object>> executeSQL(Connection conn, String sql)
                                                                                   throws SQLException {

        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);

            ResultSetMetaData medaData = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> map = new LinkedHashMap<String, Object>();
                for (int i = 1; i <= medaData.getColumnCount(); i++) {
                    map.put(medaData.getColumnName(i), rs.getObject(i));
                }
                list.add(map);
            }
        } catch (SQLException sqlException) {
            throw sqlException;
        } finally {
            closeResultSet(rs);
            closeStatement(stmt);
        }

        return list;
    }

    /**
     * 
     * 
     * @param userName
     * @param password
     * @param clusterAddress
     * @return
     * @throws SQLException
     */
    public static String getMasterLMS(String userName, String password, String clusterAddress)
                                                                                              throws SQLException {
        String masterLMSAddress = "";
        String[] processIP = clusterAddress.split(",");

        Connection connection = null;
        Statement stmt = null;
        ResultSet rs = null;

        int count = 0;
        for (String s : processIP) {
            try {
                connection = DriverManager.getConnection("jdbc:mysql://" + s, userName, password);
                stmt = connection.createStatement();
                rs = stmt.executeQuery(MASTER_CLUSTER_INFO);

                while (rs.next()) {
                    masterLMSAddress = rs.getString(1) + ":" + rs.getString(2);
                }

                break;
            } catch (SQLException e) {
                if (count >= processIP.length - 1) {
                    throw e;
                }
                logger.warn("connection error, server info:" + s);
                count++;
            }
        }

        closeResultSet(rs);
        closeStatement(stmt);
        closeConnection(connection);

        return masterLMSAddress;
    }

    /**
     * 
     * 
     * @param userName
     * @param password
     * @param url
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String userName, String password, String url)
                                                                                        throws SQLException {
        if (StringUtils.isBlank(LMS_ADDRESS)) {
            LMS_ADDRESS = getMasterLMS(userName, password, url);
        }
        return DriverManager.getConnection("jdbc:mysql://" + LMS_ADDRESS, userName, password);
    }

    public static void closeConnection(Connection conn) throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    public static void closeStatement(Statement stmt) throws SQLException {
        if (stmt != null) {
            stmt.close();
        }
    }

    public static void closeResultSet(ResultSet rs) throws SQLException {
        if (rs != null) {
            rs.close();
        }
    }

    private ObUtil() {
    }

}