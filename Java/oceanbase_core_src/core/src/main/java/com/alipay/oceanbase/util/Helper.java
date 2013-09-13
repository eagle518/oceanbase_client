package com.alipay.oceanbase.util;

import static com.alipay.oceanbase.util.OBDataSourceConstants.CONFIG_EQUALS_CHAR;
import static com.alipay.oceanbase.util.OBDataSourceConstants.CONFIG_SPLIT_CHAR;
import static com.alipay.oceanbase.util.OBDataSourceConstants.CONNECTION_PROPERTIES_KEY;
import static com.alipay.oceanbase.util.OBDataSourceConstants.MAX_ACTIVE_KEY;
import static com.alipay.oceanbase.util.OBDataSourceConstants.MIN_IDLE_KEY;
import static com.alipay.oceanbase.util.OBDataSourceConstants.MYSQL_URL_FORMAT;
import static com.alipay.oceanbase.util.OBDataSourceConstants.READ_DIST_TABLE_SIZE;
import static com.alipay.oceanbase.util.OBDataSourceConstants.REPORT_VERSION_SQL;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;

import com.alipay.oceanbase.config.ClusterConfig;
import com.alipay.oceanbase.exception.InvalidException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: Helper.java, v 0.1 2013-5-28 下午3:49:36 liangjie.li Exp $
 */
public class Helper {

    /**
     * 
     * 
     * @param dsConfig
     * @return
     */
    public static Map<String, Object> loadDsConfig(String dsConfig) {
        // will check all options
        Map<String, Object> parameters = new HashMap<String, Object>();

        if (StringUtils.isNotBlank(dsConfig)) {
            String[] allConfig = dsConfig.split(CONFIG_SPLIT_CHAR);
            if (allConfig != null && allConfig.length > 0) {

                for (String s : allConfig) {
                    String[] kv = s.split(CONFIG_EQUALS_CHAR);
                    if (kv != null && kv.length == 2) {
                        if (MAX_ACTIVE_KEY.equals(kv[0]) || MIN_IDLE_KEY.equals(kv[0])) {
                            int val = 0;
                            try {
                                val = Integer.parseInt(kv[1]);
                            } catch (NumberFormatException e) {
                                // ignore
                            }
                            parameters.put(kv[0], val);
                        } else if (CONNECTION_PROPERTIES_KEY.equals(kv[0])) {
                            parameters.put(kv[0], kv[1]);
                        }
                    }
                }
            }
        }

        return parameters;
    }

    /**
     * 
     * 
     * @param dsConfig
     * @return
     */
    public static Map<String, String> loadDsConnectionProperties(String dsConfig) {
        Map<String, String> parameters = new HashMap<String, String>();

        if (StringUtils.isNotBlank(dsConfig)) {
            // parse dsconfig
            String[] allConfig = dsConfig.split(CONFIG_SPLIT_CHAR);
            if (allConfig != null && allConfig.length > 0) {
                for (String s : allConfig) {
                    String[] kv = s.split(CONFIG_EQUALS_CHAR);
                    if (kv != null && kv.length == 2) {
                        if (CONNECTION_PROPERTIES_KEY.equals(kv[0])) {
                            parameters.put(kv[0], kv[1]);
                        }
                    }
                }
            }
        }

        return parameters;
    }

    /**
     * decrement by 1 if old value positive
     * 
     * @param v
     * @return the function returns the old value of v minus 1, even if the atomic variable, v, was not decremented.
     */
    public static int atomicDecIfPositive(AtomicInteger v) {
        if (v.get() <= 0) {// v must greater 0
            return -1;
        }
        return v.decrementAndGet();
    }

    /**
     * 
     * 
     * @param ip
     * @param port
     * @return
     */
    public static String getMySqlConURL(String ip, long port) {
        if (StringUtils.isNotBlank(ip) && port > 0) {
            return MYSQL_URL_FORMAT.format(new String[] { ip, String.valueOf(port), "Java" });
        }
        throw new InvalidException("get merge url error, ip or port is null");
    }

    /**
     * 
     * 
     * @return
     */
    public static String getVersionReportSQL() {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        Driver d = null;
        int major = 0;
        int minor = 0;
        while (drivers.hasMoreElements()) {
            d = drivers.nextElement();
            if (d.getClass().getName().contains("mysql")) {
                major = d.getMajorVersion();
                minor = d.getMinorVersion();
            }
        }
        return String.format(REPORT_VERSION_SQL, major, minor);
    }

    /**
     * 構建流量分配數組
     * 
     * @param clusters
     * @return
     */
    public static ClusterConfig[] buildReadDistTable(Set<ClusterConfig> clusters) {
        ClusterConfig[] table = new ClusterConfig[READ_DIST_TABLE_SIZE];

        int total = 0, i = 0, slot = 0, percent = 0;
        int[] readPercent = new int[clusters.size()];
        ClusterConfig masterCluster = null;

        for (ClusterConfig cc : clusters) {// 获取集群的流量\总值以及主集群
            readPercent[i] = (int) cc.getPercent();
            total += readPercent[i];

            i++;
            if (cc.isMaster()) {
                masterCluster = cc;
            }
        }

        i = 0;
        if (total > 0) {
            for (ClusterConfig cc : clusters) {
                if (readPercent[i] > 0) {
                    percent = (((readPercent[i] * READ_DIST_TABLE_SIZE) / total));
                    for (int p = 0; p < percent; ++p) {
                        table[slot++ % READ_DIST_TABLE_SIZE] = cc;
                    }
                }
                i++;
            }

            for (; slot < READ_DIST_TABLE_SIZE; ++slot) {
                table[slot] = table[slot - 1];
            }
        } else {
            for (i = 0; i < READ_DIST_TABLE_SIZE; ++i) {
                table[slot++] = masterCluster;
            }
        }

        Collections.shuffle(Arrays.asList(table));

        return table;
    }

    private Helper() {
    }
}