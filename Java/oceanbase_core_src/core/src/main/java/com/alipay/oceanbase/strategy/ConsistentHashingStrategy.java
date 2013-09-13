package com.alipay.oceanbase.strategy;

import static com.alipay.oceanbase.util.OBDataSourceConstants.LB_MODULE_LOGGER_NAME;
import static com.alipay.oceanbase.util.OBDataSourceConstants.RETRY_IN_CLUSTER_TIMES;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.exception.OceanBaseRuntimeException;
import com.alipay.oceanbase.factory.DataSourceFactory;
import com.alipay.oceanbase.factory.DataSourceHolder;
import com.alipay.oceanbase.util.MurmurHash3;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: ConsistentHashingStrategy.java, v 0.1 2013-5-24 下午3:37:03 liangjie.li Exp $
 */
public class ConsistentHashingStrategy extends EquityStrategy {
    private static final Logger                              logger             = Logger
                                                                                    .getLogger(LB_MODULE_LOGGER_NAME);

    protected Map<String, String>                            configParams       = null;
    protected Set<MergeServerConfig>                         mergeServerConfigs = null;
    protected List<DataSourceHolder>                         dataSourceHolders  = null;
    protected ConcurrentNavigableMap<Long, DataSourceHolder> buckets            = null;

    /**
     * 
     * @param dataSourceHolders
     * @throws SQLException 
     */
    public ConsistentHashingStrategy(Set<MergeServerConfig> mergeServerConfigs,
                                     Map<String, String> configParams) throws SQLException {
        this.configParams = configParams;
        this.mergeServerConfigs = mergeServerConfigs;
        this.buckets = new ConcurrentSkipListMap<Long, DataSourceHolder>();
        this.dataSourceHolders = new CopyOnWriteArrayList<DataSourceHolder>();

        for (MergeServerConfig msc : mergeServerConfigs) {
            DataSourceHolder dsh = DataSourceFactory.getHolder(msc, configParams);
            dataSourceHolders.add(dsh);

            for (int j = 0; j < 100; ++j) {
                int hashCode = MurmurHash3.murmurhash(dsh.toString() + "-" + j);
                buckets.put((long) hashCode, dsh);
            }

            if (logger.isInfoEnabled()) {
                logger.info("create one druid datasource, mergeserver: " + msc);
            }
        }
    }

    /**
     * 
     * @see com.alipay.oceanbase.strategy.EquityStrategy#select(java.util.List, java.lang.Object[])
     */
    @Override
    public DataSourceHolder select(List<DataSourceHolder> excludeKeys, Object... args) {
        if (buckets.isEmpty()) {
            throw new OceanBaseRuntimeException("no available merge server!");
        }

        String sql = this.restorePreparedStatementSQL(args);
        int hashValue = MurmurHash3.murmurhash(sql);

        DataSourceHolder holder = null;
        for (int i = 0; i < RETRY_IN_CLUSTER_TIMES; i++) {
            holder = select(hashValue, i);
            if (!excludeKeys.contains(holder) && !holder.isInvalid()) {
                return holder;
            }
        }

        return holder;
    }

    /**
     * 
     * 
     * @param hashCode
     * @param level
     * @return
     */
    protected DataSourceHolder select(int hashCode, int level) {
        if (level == 0) {
            return select(hashCode);
        }

        DataSourceHolder prev = select(hashCode);
        DataSourceHolder next = null;
        int i = level;
        int totalNum = buckets.size();
        boolean found = false;

        if (i < mergeServerConfigs.size()) {
            Long k = findPointFor(hashCode);
            Long tmpK;
            while ((i > 0) && --totalNum > 0) {
                next = select(k + 1L);

                if (logger.isDebugEnabled()) {
                    logger.debug("hashCode:" + (k + 1L) + " next:" + next);
                }

                if (!(next.equals(prev))) {
                    if ((--i) == 0) {
                        if (logger.isInfoEnabled()) {
                            logger.info("find datasource: " + next + ", times: "
                                        + (buckets.size() - totalNum));
                        }
                        found = true;
                        break;
                    }
                }
                tmpK = findPointFor(k + 1L);
                k = tmpK;
            }
        }

        if (!found) {
            logger.warn("can not find any datasource. level: " + level + ", totalNum: "
                        + buckets.size() + ", number: " + mergeServerConfigs.size());
            next = null;
        }
        return next;
    }

    /**
     * 
     * 
     * @param l
     * @return
     */
    protected DataSourceHolder select(long l) {
        Long k = findPointFor(l);
        return buckets.get(k);
    }

    /**
     * 
     * 
     * @param hashCode
     * @return
     */
    protected Long findPointFor(long hashCode) {
        Long k = buckets.ceilingKey((long) hashCode);
        // if none found, it must be at the end, return the lowest in the tree (we go over the end the continuum to the first entry)
        if (k == null) {
            k = buckets.firstKey();
        }
        return k;
    }

    /**
     * 
     * 
     * @param args
     * @return
     */
    protected String restorePreparedStatementSQL(Object... args) {
        if (args.length > 0) {
            String sql = (String) args[0];

            if (args.length > 1) {
                StringBuilder sqlWithParamters = new StringBuilder(sql);
                sqlWithParamters.append(args[1]);
                sql = sqlWithParamters.toString();
            }

            return sql;
        }

        throw new IllegalArgumentException("ps sql error!");
    }

    /**
     * 
     * @see com.alipay.oceanbase.strategy.EquityStrategy#isInvalid()
     */
    @Override
    public boolean isInvalid() {
        return super.isInvalid(dataSourceHolders);
    }

    /**
     * 
     * @see com.alipay.oceanbase.strategy.EquityStrategy#destroyDataSource()
     */
    @Override
    public void destroyDataSource() {
        super.destroyDataSource(dataSourceHolders);
        dataSourceHolders.clear();
        buckets.clear();

        if (logger.isInfoEnabled()) {
            logger.info("destroy all druid datasource, mergeservers:" + this.mergeServerConfigs);
        }
    }

    /**
     * 
     * @see com.alipay.oceanbase.strategy.EquityStrategy#destroyDataSource(com.alipay.oceanbase.config.MergeServerConfig)
     */
    @Override
    public void destroyDataSource(MergeServerConfig msc) {
        DataSourceHolder dsh = null;
        for (int j = 0; j < 100; ++j) {
            int hashCode = MurmurHash3.murmurhash(msc.toString() + "-" + j);
            dsh = this.buckets.remove((long) hashCode);
        }

        dataSourceHolders.remove(dsh);
        dsh.destroy();
    }

    /**
     * 
     * @throws SQLException 
     * @see com.alipay.oceanbase.strategy.EquityStrategy#addDataSource(com.alipay.oceanbase.config.MergeServerConfig)
     */
    @Override
    public void addDataSource(MergeServerConfig msc) throws SQLException {
        DataSourceHolder dsh = DataSourceFactory.getHolder(msc, configParams);
        dataSourceHolders.add(dsh);

        for (int j = 0; j < 100; ++j) {
            int hashCode = MurmurHash3.murmurhash(dsh.toString() + "-" + j);
            this.buckets.put((long) hashCode, dsh);
        }
    }

    /**
     * 
     * @see com.alipay.oceanbase.strategy.EquityStrategy#printDSStatus()
     */
    @Override
    public void printDSStatus() {
        super.printDSStatus(dataSourceHolders);
    }

    /**
     * 
     * @see com.alipay.oceanbase.strategy.EquityStrategy#reloadDataSources(int, int, java.lang.String)
     */
    @Override
    protected void reloadDataSources(Integer maxActive, Integer minIdle, String connectionProperties) {
        for (DataSourceHolder dsh : dataSourceHolders) {
            this.reloadDataSources(maxActive, minIdle, connectionProperties, dsh);
        }
    }

}
