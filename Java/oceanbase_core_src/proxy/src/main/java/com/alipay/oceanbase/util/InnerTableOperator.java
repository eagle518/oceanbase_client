package com.alipay.oceanbase.util;

import static com.alipay.oceanbase.log.CommonLoggerComponent.PORXY_MODULE_LOGGER_NAME;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: InnerTableOperator.java, v 0.1 Jun 6, 2013 12:01:53 PM liangjieli Exp $
 */
public class InnerTableOperator {

    private static final Logger logger            = Logger.getLogger(PORXY_MODULE_LOGGER_NAME);

    private static final String CLIENT_REPORT_SQL = "replace into __all_client (client_ip, version) values (?,?)";

    /**
     * report client version
     * 
     * @param datasource
     * @param coreJarVersion
     */
    public static void reportCoreJarVersion(DataSource datasource, String coreJarVersion) {
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            String localIp = InetAddress.getLocalHost().getHostAddress();
            conn = datasource.getConnection();
            pstmt = conn.prepareStatement(CLIENT_REPORT_SQL);
            pstmt.setString(1, localIp);
            pstmt.setString(2, coreJarVersion);
            pstmt.execute();

            if (logger.isInfoEnabled()) {
                logger.info("report core jar version, local ip: " + localIp + ", version:"
                            + coreJarVersion);
            }
        } catch (Throwable ex) {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("report CoreJar Version[%s] fail", coreJarVersion), ex);
            }
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    logger.error("", e);
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error("", e);
                }
            }
        }
    }

    private InnerTableOperator() {
    }
}
