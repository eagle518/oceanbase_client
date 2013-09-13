package com.alipay.oceanbase.util;

import java.text.MessageFormat;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: Constants.java, v 0.1 2013-5-24 下午3:26:19 liangjie.li Exp $
 */
public class OBDataSourceConstants {

    public static final MessageFormat MYSQL_URL_FORMAT               = new MessageFormat(
                                                                         "jdbc:mysql://{0}:{1}/{2}");

    public static final int           RETRY_IN_CLUSTER_TIMES         = 2;
    public static final int           READ_DIST_TABLE_SIZE           = 100;
    public static final int           CLUSTER_INVALID_TIME           = 60000;                                                                                                                                                            //60s
    public static final int           MURMURHASH_M                   = 0x9747b28c;

    public static final char          SPLIT_CHAR                     = ';';

    public static final long          MASTER                         = 1;
    public static final long          SLAVE                          = 2;

    public static final double        AUDIT_FACTOR                   = 0.9999;
    public static final double        AUDIT_THRESHOLD                = 47.63;
    public static final double        AUDIT_SUCCESS                  = 0.0;
    public static final double        AUDIT_FAILURE                  = 1.0;

    public static final String        CHECK_VALID_CONNECTION_SQL     = "select 0";
    public static final String        USER_NAME                      = "username";
    public static final String        PASSWORD                       = "password";
    public static final String        CLUSTER_ADDRESS                = "clusterAddress";
    public static final String        DS_CONFIG                      = "dsConfig";
    public static final String        PERIOD                         = "period";
    public static final String        BLANK                          = "";
    public static final String        CONFIG_SPLIT_CHAR              = ",";
    public static final String        CONFIG_EQUALS_CHAR             = ":";
    public static final String        MAX_ACTIVE_KEY                 = "maxActive";
    public static final String        MIN_IDLE_KEY                   = "minIdle";
    public static final String        CONNECTION_PROPERTIES_KEY      = "connectionProperties";

    public static final String        DEFAULT_MYSQL_DRIVER_CLASS     = "com.mysql.jdbc.Driver";
    public static final String        LB_MODULE_LOGGER_NAME          = "lbModuleLogger";
    public static final String        DS_STATUS_MODULE_LOGGER_NAME   = "dsStatusModuleLogger";
    public static final String        COMMON_LOGGER_NAME             = "com.alipay.oceanbase";
    public static final String        DAEMON_TASK_MODULE_LOGGER_NAME = "daemonTaskModuleLogger";
    public static final String        REPORT_VERSION_SQL             = "select /*+client(obdatasource) client_version(1.2.1) mysql_driver(%d.%d)*/ \'client_version\'";
    public static final String        CONNECTION_PROPERTIES          = "emulateUnsupportedPstmts=false;characterEncoding=GBK;useServerPrepStmts=true;prepStmtCacheSqlLimit=1000;useLocalSessionState=true;useLocalTransactionState=true";

    private OBDataSourceConstants() {
    }
}
