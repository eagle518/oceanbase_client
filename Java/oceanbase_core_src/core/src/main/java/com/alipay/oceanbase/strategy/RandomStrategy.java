package com.alipay.oceanbase.strategy;

import static com.alipay.oceanbase.util.OBDataSourceConstants.LB_MODULE_LOGGER_NAME;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.exception.OceanBaseRuntimeException;
import com.alipay.oceanbase.factory.DataSourceFactory;
import com.alipay.oceanbase.factory.DataSourceHolder;
import com.alipay.oceanbase.util.ThreadLocalRandom;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: RandomStrategy.java, v 0.1 2013-5-24 下午3:31:04 liangjie.li Exp $
 */
public class RandomStrategy extends EquityStrategy {

    private static final Logger      logger             = Logger.getLogger(LB_MODULE_LOGGER_NAME);

    protected Map<String, String>    configParams       = null;
    protected Set<MergeServerConfig> mergeServerConfigs = null;
    protected List<DataSourceHolder> buckets            = null;

    public RandomStrategy(Set<MergeServerConfig> mergeServerConfigs,
                          Map<String, String> configParams) throws SQLException {
        this.configParams = configParams;
        this.mergeServerConfigs = mergeServerConfigs;
        this.buckets = new CopyOnWriteArrayList<DataSourceHolder>();

        for (MergeServerConfig msc : mergeServerConfigs) {
            DataSourceHolder dsh = DataSourceFactory.getHolder(msc, configParams);
            this.buckets.add(dsh);

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
        DataSourceHolder holder = buckets.get(ThreadLocalRandom.current().nextInt(buckets.size()));

        int i = 1;
        while ((excludeKeys.contains(holder) || holder.isInvalid()) && i < buckets.size()
               && buckets.size() != excludeKeys.size()) {

            logger.warn("retry to choose another one ms, this holder is fail, " + holder);
            holder = buckets.get(ThreadLocalRandom.current().nextInt(buckets.size()));

            i++;
        }
        return holder;
    }

    /**
     * 
     * @see com.alipay.oceanbase.strategy.EquityStrategy#isInvalid()
     */
    @Override
    public boolean isInvalid() {
        return super.isInvalid(buckets);
    }

    /**
     * 
     * @see com.alipay.oceanbase.strategy.EquityStrategy#destroyDataSource()
     */
    @Override
    public void destroyDataSource() {
        super.destroyDataSource(buckets);
        this.buckets.clear();

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
        for (DataSourceHolder dsh : buckets) {
            if (dsh.toString().equals(msc.toString())) {
                buckets.remove(dsh);
                dsh.destroy();
            }
        }
    }

    /**
     * 
     * @throws SQLException 
     * @see com.alipay.oceanbase.strategy.EquityStrategy#addDataSource(com.alipay.oceanbase.config.MergeServerConfig)
     */
    @Override
    public void addDataSource(MergeServerConfig msc) throws SQLException {
        DataSourceHolder dsh = DataSourceFactory.getHolder(msc, this.configParams);
        buckets.add(dsh);
    }

    /**
     * 
     * @see com.alipay.oceanbase.strategy.EquityStrategy#printDSStatus()
     */
    @Override
    public void printDSStatus() {
        super.printDSStatus(buckets);
    }

    @Override
    protected void reloadDataSources(Integer maxActive, Integer minIdle, String connectionProperties) {
        for (DataSourceHolder dsh : buckets) {
            this.reloadDataSources(maxActive, minIdle, connectionProperties, dsh);
        }
    }

}
