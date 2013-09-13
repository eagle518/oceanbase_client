package com.alipay.oceanbase.strategy;

import static com.alipay.oceanbase.util.OBDataSourceConstants.LB_MODULE_LOGGER_NAME;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.exception.OceanBaseRuntimeException;
import com.alipay.oceanbase.factory.DataSourceHolder;
import com.alipay.oceanbase.util.ThreadLocalSequenceNumber;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: RoundRobinStrategy.java, v 0.1 Jun 27, 2013 3:50:21 PM liangjie.li Exp $
 */
public class RoundRobinStrategy extends RandomStrategy {

    private static final Logger logger = Logger.getLogger(LB_MODULE_LOGGER_NAME);

    public RoundRobinStrategy(Set<MergeServerConfig> mergeServerConfigs,
                              Map<String, String> configParams) throws SQLException {
        super(mergeServerConfigs, configParams);
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

        DataSourceHolder holder = buckets.get(Math.abs(ThreadLocalSequenceNumber
            .getSequenceNumber()) % buckets.size());

        int i = 1;
        while ((excludeKeys.contains(holder) || holder.isInvalid()) && i < buckets.size()
               && buckets.size() != excludeKeys.size()) {

            logger.warn("retry to choose another one ms, this holder is fail, " + holder);
            holder = buckets.get(ThreadLocalSequenceNumber.getSequenceNumber() % buckets.size());

            i++;
        }
        return holder;
    }
}
