package com.alipay.oceanbase.strategy;

import static com.alipay.oceanbase.util.OBDataSourceConstants.CONNECTION_PROPERTIES_KEY;
import static com.alipay.oceanbase.util.OBDataSourceConstants.DS_STATUS_MODULE_LOGGER_NAME;
import static com.alipay.oceanbase.util.OBDataSourceConstants.LB_MODULE_LOGGER_NAME;
import static com.alipay.oceanbase.util.OBDataSourceConstants.MAX_ACTIVE_KEY;
import static com.alipay.oceanbase.util.OBDataSourceConstants.MIN_IDLE_KEY;
import static com.alipay.oceanbase.util.OBDataSourceConstants.SPLIT_CHAR;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.alibaba.druid.pool.DruidDataSource;
import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.factory.DataSourceHolder;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: EquityStrategy.java, v 0.1 2013-5-14 下午4:58:46 liangjie.li Exp $
 */
public abstract class EquityStrategy {

    private static final Logger _logger  = Logger.getLogger(DS_STATUS_MODULE_LOGGER_NAME);
    private static final Logger __logger = Logger.getLogger(LB_MODULE_LOGGER_NAME);

    /**
     * 
     * 
     * @param excludeKeys
     * @param sql
     * @return
     */
    public abstract DataSourceHolder select(List<DataSourceHolder> excludeKeys, Object... objs);

    /**
     * 
     * 
     * @param parameters
     */
    public void reloadDataSources(Map<String, Object> parameters) {
        Integer maxActive = (Integer) parameters.get(MAX_ACTIVE_KEY);
        Integer minIdle = (Integer) parameters.get(MIN_IDLE_KEY);
        String connectionProperties = (String) parameters.get(CONNECTION_PROPERTIES_KEY);

        this.reloadDataSources(maxActive, minIdle, connectionProperties);
    }

    /**
     * 
     * 
     * @param maxActive
     * @param minIdle
     * @param connectionProperties
     */
    protected abstract void reloadDataSources(Integer maxActive, Integer minIdle,
                                              String connectionProperties);

    /**
     * 
     * 
     * @param maxActive
     * @param minIdle
     * @param connectionProperties
     * @param dsh
     */
    protected void reloadDataSources(Integer maxActive, Integer minIdle,
                                     String connectionProperties, DataSourceHolder dsh) {
        if (maxActive != null && maxActive > 0) {
            dsh.setMaxActive(maxActive);
        }
        if (minIdle != null && minIdle > 0) {
            dsh.setMinIdle(minIdle);
        }
        if (StringUtils.isNotBlank(connectionProperties)) {
            dsh.setConnectionProperties(connectionProperties);
        }
    }

    /**
     * 
     */
    public abstract boolean isInvalid();

    /**
     * 
     * 
     * @param buckets
     * @return
     */
    protected boolean isInvalid(Collection<DataSourceHolder> buckets) {
        for (DataSourceHolder dsh : buckets) {
            if (!dsh.isInvalid()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 
     */
    public abstract void destroyDataSource();

    protected void destroyDataSource(Collection<DataSourceHolder> buckets) {
        for (DataSourceHolder dsh : buckets) {
            if (__logger.isInfoEnabled()) {
                __logger.info("will destroy druid datasource, mergeserver:" + dsh);
            }

            dsh.destroy();
        }
    }

    /**
     * 
     * 
     * @param msc
     */
    public abstract void destroyDataSource(MergeServerConfig msc);

    /**
     * 
     * 
     * @param msc
     */
    public abstract void addDataSource(MergeServerConfig msc) throws SQLException;

    /**
     * 
     */
    public abstract void printDSStatus();

    protected void printDSStatus(Collection<DataSourceHolder> buckets) {
        for (DataSourceHolder dsh : buckets) {
            this.printDSStatus((DruidDataSource) dsh.getDataSource());
        }
    }

    /**
     * name;CreateCount;DestroyCount;CreateErrorCount;ConnectCount;ConnectErrorCount;CloseCount;ActiveCount;ActivePeak;PoolingCount;LockQueueLength;WaitThreadCount;InitialSize;MaxActive;MinIdle;StartTransactionCount;CommitCount;RollbackCount;ErrorCount;ReusePreparedStatementCount
     * 
     * @param ds
     */
    public void printDSStatus(DruidDataSource ds) {
        if (_logger.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append(ds.getName().replace('_', ':')).append(SPLIT_CHAR)
            // Name;
                .append(ds.getCreateCount()).append(SPLIT_CHAR)
                // 1.CreateCount
                .append(ds.getDestroyCount()).append(SPLIT_CHAR)
                // 2.DestroyCount
                .append(ds.getCreateErrorCount()).append(SPLIT_CHAR)
                // 3.CreateErrorCount
                .append(ds.getConnectCount()).append(SPLIT_CHAR)
                // 4.ConnectCount
                .append(ds.getConnectErrorCount()).append(SPLIT_CHAR)
                // 5.ConnectErrorCount
                .append(ds.getCloseCount()).append(SPLIT_CHAR)
                // 6.CloseCount
                .append(ds.getActiveCount()).append(SPLIT_CHAR)
                // 7.ActiveCount
                .append(ds.getActivePeak()).append(SPLIT_CHAR)
                // 8.ActivePeak
                .append(ds.getPoolingCount()).append(SPLIT_CHAR)
                // 9.PoolingCount
                .append(ds.getLockQueueLength()).append(SPLIT_CHAR)
                // 10.LockQueueLength
                .append(ds.getWaitThreadCount()).append(SPLIT_CHAR)
                // 11.WaitThreadCount
                .append(ds.getInitialSize()).append(SPLIT_CHAR)
                // 12.InitialSize
                .append(ds.getMaxActive()).append(SPLIT_CHAR)
                // 13.MaxActive
                .append(ds.getMinIdle()).append(SPLIT_CHAR)
                // 14.MinIdle
                .append(ds.getStartTransactionCount()).append(SPLIT_CHAR)
                // 15.StartTransactionCount
                .append(ds.getCommitCount()).append(SPLIT_CHAR)
                // 16.CommitCount
                .append(ds.getRollbackCount()).append(SPLIT_CHAR)
                // 17.RollbackCount
                .append(ds.getErrorCount()).append(SPLIT_CHAR)
                // 18.ErrorCount
                .append(ds.getCachedPreparedStatementHitCount()).append(SPLIT_CHAR)
                // 19.CachedPreparedStatementHitCount
                .append(ds.getCachedPreparedStatementMissCount());
            // 20.CachedPreparedStatementMissCount
            _logger.info(sb.toString());
        }
    }
}
