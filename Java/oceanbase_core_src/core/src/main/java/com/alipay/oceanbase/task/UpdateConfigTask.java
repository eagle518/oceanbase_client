package com.alipay.oceanbase.task;

import static com.alipay.oceanbase.util.OBDataSourceConstants.BLANK;
import static com.alipay.oceanbase.util.OBDataSourceConstants.CLUSTER_ADDRESS;
import static com.alipay.oceanbase.util.OBDataSourceConstants.DAEMON_TASK_MODULE_LOGGER_NAME;
import static com.alipay.oceanbase.util.OBDataSourceConstants.DS_CONFIG;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.alipay.oceanbase.OBGroupDataSource;
import com.alipay.oceanbase.config.ClusterConfig;
import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.config.OBDataSourceConfig;
import com.alipay.oceanbase.group.EquityMSManager;
import com.alipay.oceanbase.util.ConfigLoader;
import com.alipay.oceanbase.util.Helper;
import com.alipay.oceanbase.util.ObUtil;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: UpdateConfigTask.java, v 0.1 2013-5-24 下午4:15:00 liangjie.li Exp $
 */
public class UpdateConfigTask implements Runnable {

    private static final Logger       logger         = Logger
                                                         .getLogger(DAEMON_TASK_MODULE_LOGGER_NAME);
    static final String               LOCAL_JAR_PATH = System.getProperty("user.home")
                                                       + "/.obdatasource/conf";

    private final String              configUrl;
    private final String              userName;
    private final String              password;

    private final Map<String, String> configParams;
    private final OBGroupDataSource   obGroupDataSource;

    private OBDataSourceConfig        preDSConfig    = null;

    public UpdateConfigTask(String userName, String password, String configUrl,
                            OBDataSourceConfig obDataSourceConfig,
                            Map<String, String> configParams, OBGroupDataSource obGroupDataSource) {
        this.preDSConfig = obDataSourceConfig;

        this.configUrl = configUrl;
        this.userName = userName;
        this.password = password;
        this.configParams = configParams;
        this.obGroupDataSource = obGroupDataSource;
    }

    /**
     * 
     * @see java.lang.Runnable#run()
     */
    public void run() {
        if (logger.isDebugEnabled()) {
            logger.debug("running...");
        }

        Properties properties = ConfigLoader.load(this.configUrl);
        String tempClusterAddress = properties.getProperty(CLUSTER_ADDRESS, BLANK);
        String tempDsConfig = properties.getProperty(DS_CONFIG, BLANK);

        if (StringUtils.isNotBlank(tempClusterAddress)) {
            this.compareServerInfo(tempClusterAddress);
        }
        if (StringUtils.isNotBlank(tempDsConfig)) {
            this.reloadDataSource(tempDsConfig);
        }
    }

    /**
     * 
     * 
     * @param clusterAddress
     */
    public void compareServerInfo(String clusterAddress) {
        try {
            Connection conn = ObUtil.getConnection(userName, password, clusterAddress);
            Set<ClusterConfig> currentConfig = ObUtil.initClusterInfo(userName, password, conn,
                configParams);
            ObUtil.closeConnection(conn);

            // compare cluster
            boolean isClusterChange = compareCluster(currentConfig, preDSConfig.getClusterConfigs());
            if (isClusterChange) {// reinit all config
                OBDataSourceConfig newDSConfig = new OBDataSourceConfig(userName, password,
                    clusterAddress, configParams);

                EquityMSManager equityMSManager = new EquityMSManager(newDSConfig, this);
                this.obGroupDataSource.setDBSelector(equityMSManager);
                this.obGroupDataSource.setConfig(newDSConfig);

                this.preDSConfig.destroyAllDruidDS();
                this.preDSConfig = newDSConfig;

                logger.warn("cluster info has changed, so rebuild group datasource!");

                return;
            }

            // compare percent
            boolean isClusterPercentChange = comparePercent(currentConfig,
                preDSConfig.getClusterConfigs());
            if (isClusterPercentChange) {
                for (ClusterConfig clusterConfig : preDSConfig.getClusterConfigs()) {
                    for (ClusterConfig cc : currentConfig) {
                        if (clusterConfig.equals(cc)) {
                            clusterConfig.setPercent(cc.getPercent());
                        }
                    }
                }
                ClusterConfig[] newClusterConfigs = Helper.buildReadDistTable(preDSConfig
                    .getClusterConfigs());
                this.obGroupDataSource.getDBSelector().setReadDistTable(newClusterConfigs);

                logger
                    .warn("cluster percent has changed. old:"
                          + preDSConfig.getClusterConfigs().toString() + "; new:" + currentConfig);
            }

            // compare mergeservers 
            for (ClusterConfig _cc : preDSConfig.getClusterConfigs()) {
                for (ClusterConfig cc : currentConfig) {
                    if (cc.equals(_cc)) {
                        compareAndChangeMergeServer(cc.getServers(), _cc.getServers(), _cc);
                    }
                }
            }
        } catch (SQLException e) {
            logger.warn(
                "ignore, update mergerserver list or cluster info error! exception: SQLException",
                e);
        }
    }

    /**
     * 
     * 
     * @param dsConfig
     */
    public void reloadDataSource(String dsConfig) {
        Map<String, Object> map = Helper.loadDsConfig(dsConfig);
        if (map != null && !map.isEmpty()) {
            this.preDSConfig.reloadDataSources(map);
        }
    }

    /**
     * 
     * 
     * @param sets
     * @param preSets
     * @return
     */
    static boolean compareCluster(Set<ClusterConfig> sets, Set<ClusterConfig> preSets) {
        if (sets.size() == preSets.size()) {// same 
            for (ClusterConfig cc : sets) {
                if (!preSets.contains(cc)) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    /**
     * 
     * 
     * @param sets
     * @param preSets
     * @return
     */
    static boolean comparePercent(Set<ClusterConfig> sets, Set<ClusterConfig> preSets) {
        for (ClusterConfig cc : sets) {
            for (ClusterConfig _cc : preSets) {
                if (cc.equals(_cc) && cc.isPercentChange(_cc)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 
     * 
     * @param sets
     * @param preSets
     * @throws SQLException 
     */
    static void compareAndChangeMergeServer(Set<MergeServerConfig> currSets,
                                            Set<MergeServerConfig> preSets, ClusterConfig cc)
                                                                                             throws SQLException {
        List<MergeServerConfig> missList = new ArrayList<MergeServerConfig>();
        List<MergeServerConfig> addList = new ArrayList<MergeServerConfig>();

        for (MergeServerConfig msc : currSets) {
            if (!preSets.contains(msc)) {
                addList.add(msc);
            }
        }

        for (MergeServerConfig msc : preSets) {
            if (!currSets.contains(msc)) {
                missList.add(msc);
            }
        }

        for (MergeServerConfig msc : missList) {
            logger.warn("remove datasource, mergeserver: " + msc);
            cc.getServers().remove(msc);
            cc.getEquityStrategy().destroyDataSource(msc);
        }

        for (MergeServerConfig msc : addList) {
            logger.warn("add datasource, mergeserver: " + msc);
            cc.getServers().add(msc);
            cc.getEquityStrategy().addDataSource(msc);
        }
    }
}
