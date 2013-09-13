package com.alipay.oceanbase.group;

import static com.alipay.oceanbase.util.OBDataSourceConstants.LB_MODULE_LOGGER_NAME;
import static com.alipay.oceanbase.util.OBDataSourceConstants.MASTER;
import static com.alipay.oceanbase.util.OBDataSourceConstants.READ_DIST_TABLE_SIZE;
import static com.alipay.oceanbase.util.OBDataSourceConstants.RETRY_IN_CLUSTER_TIMES;
import static com.alipay.oceanbase.util.OBDataSourceConstants.SLAVE;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.alibaba.druid.pool.DataSourceDisableException;
import com.alipay.oceanbase.config.ClusterConfig;
import com.alipay.oceanbase.config.OBDataSourceConfig;
import com.alipay.oceanbase.factory.DataSourceHolder;
import com.alipay.oceanbase.jdbc.sorter.OceanbaseBaseExceptionSorter;
import com.alipay.oceanbase.task.UpdateConfigTask;
import com.alipay.oceanbase.util.Helper;
import com.alipay.oceanbase.util.parse.SqlHintType;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: EquityDbManager.java, v 0.1 2013-6-3 下午4:32:09 liangjie.li Exp $
 */
public class EquityMSManager implements MergeServerSelector {

    private static final Logger        logger               = Logger
                                                                .getLogger(LB_MODULE_LOGGER_NAME);
    private static final ReentrantLock lock                 = new ReentrantLock();
    private static volatile long       lastCompareTimestamp = 0L;
    private static final long          TIME                 = 1 * 1000;

    private volatile ClusterConfig[]   readDistTable        = new ClusterConfig[READ_DIST_TABLE_SIZE];

    private AtomicInteger              masterCount          = new AtomicInteger(0);
    private AtomicInteger              clusterIndex         = new AtomicInteger(0);

    private ClusterConfig              masterCluster        = null;
    private ClusterConfig              slaveCluster         = null;

    private Set<ClusterConfig>         clusters             = null;

    private final UpdateConfigTask     task;

    public EquityMSManager(OBDataSourceConfig obDataSourceConfig, UpdateConfigTask task) {
        this.clusters = obDataSourceConfig.getClusterConfigs();
        this.task = task;

        Set<ClusterConfig> clusterConfigs = obDataSourceConfig.getClusterConfigs();
        for (ClusterConfig cc : clusterConfigs) {
            if (cc.getRole() == MASTER) {
                masterCluster = cc;
            } else if (cc.getRole() == SLAVE) {
                slaveCluster = cc;
            }
        }

        this.readDistTable = Helper.buildReadDistTable(clusters);
    }

    /**
     * 
     * @see com.alipay.oceanbase.group.MergeServerSelector#tryExecute(com.alipay.oceanbase.group.MergeServerSelector.DataSourceTryer, boolean, java.lang.Object[])
     */
    @Override
    public <T> T tryExecute(DataSourceTryer<T> tryer, boolean isConsistency,
                            SqlHintType whichCluster, Object... args) throws SQLException {

        ClusterConfig clusterConfig = this.selectCluster(isConsistency, whichCluster);
        List<DataSourceHolder> excludeKeys = new ArrayList<DataSourceHolder>();

        SQLException exception = null;
        for (int i = 0; i < RETRY_IN_CLUSTER_TIMES; i++) {
            DataSourceHolder dataSourceHolder = clusterConfig.getEquityStrategy().select(
                excludeKeys, args);

            if (logger.isDebugEnabled()) {
                logger.debug("sql will send to " + dataSourceHolder);
            }

            try {
                T ret = tryer.tryOnDataSource(dataSourceHolder, args);
                dataSourceHolder.audit(0);// 该数据源可用概率增加

                return ret;
            } catch (DataSourceDisableException dde) {
                logger.warn("druid datasource has destroy, will retry.");
                i--;
            } catch (SQLException e) {
                exception = e;

                boolean isFatal = OceanbaseBaseExceptionSorter.isExceptionFatal(e);
                boolean isNotMaster = OceanbaseBaseExceptionSorter.isNotMasterClusterFatal(e);

                if (!isNotMaster && !isFatal) {
                    break;
                }

                boolean lockAcquired = false;
                try {
                    if (!lock.isLocked()) {
                        if (lock.tryLock()) {
                            long currentTimestamp = System.currentTimeMillis();
                            if (currentTimestamp - lastCompareTimestamp > TIME) {// 立即更新内部表映射的数据源 
                                task.run();
                                lastCompareTimestamp = currentTimestamp;
                            }

                            lockAcquired = true;
                        }
                    }
                } catch (Exception ex) {
                    logger.error("", ex);
                } finally {
                    if (lockAcquired) {
                        lock.unlock();
                    }
                }

                if (isFatal) {
                    excludeKeys.add(dataSourceHolder);
                    dataSourceHolder.audit(1);// 该数据源不可用概率增加
                }

                if (isNotMaster) {
                    clusterConfig = this.selectCluster(isConsistency, whichCluster);
                }

                logger.warn("try locate on [" + dataSourceHolder + "] failed, ", e);
            }
        }

        throw exception;
    }

    /**
     * 1. 如果非一致性读请求（select sql或weak hint sql），进入选择，否则直接返回主，并给主增加计数，不参与选择。<br/>
     * 2. 选择规则：<br/>
     *    a. 如果选择任意的集群为不可用状态，则重选，当所有集群皆都不可用，则默认返回主；<br/>
     *    b. 如果选择主，则查看主集群计数值是否为小于或者等于0，否则重新选择；<br/>
     *    c. 如果选择备，则返回。<br/>
     * 3. 新增hint指定主备集群查询。 
     *    /*+read_cluster(master)* / 
     *    /*+read_cluster(slave)* /
     * 
     * @param isConsistency
     * @return
     */
    protected ClusterConfig selectCluster(boolean isConsistency, SqlHintType whichCluster) {
        if (SqlHintType.CLUSTER_MASTER == whichCluster) {
            if (logger.isDebugEnabled()) {
                logger.debug("sql will send to master cluster");
            }
            masterCount.incrementAndGet();
            return masterCluster;
        } else if (SqlHintType.CLUSTER_SLAVE == whichCluster) {
            if (logger.isDebugEnabled()) {
                logger.debug("sql will send to slave cluster");
            }
            return slaveCluster;
        }

        if (isConsistency) {
            if (logger.isDebugEnabled()) {
                logger.debug("sql will send to master cluster");
            }
            masterCount.incrementAndGet();
            return masterCluster;
        }

        boolean found = false;
        while (true) {
            ClusterConfig cluster = null;
            for (int i = 0; i < READ_DIST_TABLE_SIZE / 2; ++i) {
                cluster = readDistTable[Math.abs(clusterIndex.getAndIncrement()
                                                 % READ_DIST_TABLE_SIZE)];
                if (!cluster.isInvalid()) {// 选择的集群可用，并且不在requested中
                    found = true;
                    break;
                }
            }

            if (slaveCluster != null && slaveCluster.getPercent() > 0L && cluster.isMaster()
                && Helper.atomicDecIfPositive(masterCount) >= 0) {// 如果选中主，则取决主集群的总请求数是否大于等于0；是，则重新选择
                continue;
            }

            if (!found) {
                logger
                    .error("all cluster is invalid, but sql will send to master cluster, please check!");
                return masterCluster;
            }

            return cluster;
        }
    }

    /**
     * 
     * @see com.alipay.oceanbase.group.MergeServerSelector#setReadDistTable(com.alipay.oceanbase.config.ClusterConfig[])
     */
    @Override
    public void setReadDistTable(ClusterConfig[] readDistTable) {
        this.readDistTable = readDistTable;
    }
}
