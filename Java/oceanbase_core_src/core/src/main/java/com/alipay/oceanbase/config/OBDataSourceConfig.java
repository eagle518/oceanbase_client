package com.alipay.oceanbase.config;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alipay.oceanbase.strategy.ConsistentHashingStrategy;
import com.alipay.oceanbase.strategy.RandomStrategy;
import com.alipay.oceanbase.strategy.RoundRobinStrategy;
import com.alipay.oceanbase.strategy.WeakConsistencyStrategy;
import com.alipay.oceanbase.util.ObUtil;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: OBDataSourceConfig.java, v 0.1 2013-5-24 下午4:40:45 liangjie.li Exp $
 */
public class OBDataSourceConfig {

    private static final Logger       logger = Logger.getLogger(OBDataSourceConfig.class);

    private final Set<ClusterConfig>  clusterConfigs;
    private final Map<String, String> configParams;

    public OBDataSourceConfig(String userName, String password, String cluserAddress,
                              Map<String, String> configParams) throws SQLException {
        this.configParams = configParams;

        Connection conn = ObUtil.getConnection(userName, password, cluserAddress);

        Set<ClusterConfig> clusterConfigs = ObUtil.getClusterList(userName, password, conn);

        for (ClusterConfig cc : clusterConfigs) {
            // 1. get all mergeserver per cluster
            Set<MergeServerConfig> mergeServerConfigs = ObUtil.getServerList(conn,
                cc.getClusterid());
            cc.setServers(mergeServerConfigs);

            // 2. init all druid ds for every mergeserver 
            WeakConsistencyStrategy strategy = cc.getReadStrategy();
            if (strategy == WeakConsistencyStrategy.CONSISTENT_HASHING_STRATEGY) {
                ConsistentHashingStrategy chs = new ConsistentHashingStrategy(mergeServerConfigs,
                    configParams);
                cc.setEquityStrategy(chs);
            } else if (strategy == WeakConsistencyStrategy.RANDOM_STRATEGY) {
                RandomStrategy rs = new RandomStrategy(mergeServerConfigs, configParams);
                cc.setEquityStrategy(rs);
            } else if (strategy == WeakConsistencyStrategy.ROUNDROBIN_STRATEGY) {
                RoundRobinStrategy rrs = new RoundRobinStrategy(mergeServerConfigs, configParams);
                cc.setEquityStrategy(rrs);
            }

            if (logger.isInfoEnabled()) {
                logger.info("cluster info {" + cc + "}");
            }
        }
        ObUtil.closeConnection(conn);

        this.clusterConfigs = clusterConfigs;
    }

    /**
     * 
     */
    public void destroyAllDruidDS() {
        for (ClusterConfig cc : clusterConfigs) {
            cc.getEquityStrategy().destroyDataSource();
        }
    }

    /**
     * 
     * 
     * @param parameters
     */
    public void reloadDataSources(Map<String, Object> parameters) {
        if (logger.isInfoEnabled()) {
            logger.info("will reload all datasource, parameters:" + parameters);
        }
        for (ClusterConfig cc : clusterConfigs) {
            cc.getEquityStrategy().reloadDataSources(parameters);
        }
    }

    public Map<String, String> getConfigParams() {
        return configParams;
    }

    public Set<ClusterConfig> getClusterConfigs() {
        return clusterConfigs;
    }

}
