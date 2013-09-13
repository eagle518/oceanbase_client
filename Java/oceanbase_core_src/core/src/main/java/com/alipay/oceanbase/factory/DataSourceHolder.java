package com.alipay.oceanbase.factory;

import static com.alipay.oceanbase.util.OBDataSourceConstants.AUDIT_FACTOR;
import static com.alipay.oceanbase.util.OBDataSourceConstants.AUDIT_THRESHOLD;
import static com.alipay.oceanbase.util.OBDataSourceConstants.CLUSTER_INVALID_TIME;
import static com.alipay.oceanbase.util.OBDataSourceConstants.LB_MODULE_LOGGER_NAME;

import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.alibaba.druid.pool.DruidDataSource;
import com.alipay.oceanbase.config.MergeServerConfig;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: DataSourceHolder.java, v 0.1 2013-5-24 下午3:59:04 liangjie.li Exp $
 */
public abstract class DataSourceHolder {
    private static final Logger logger            = Logger.getLogger(LB_MODULE_LOGGER_NAME);

    private ReentrantLock       lock              = new ReentrantLock();

    private volatile long       invalidTime       = 0;
    private volatile double     auditValue        = 0;

    private MergeServerConfig   mergeServerConfig = null;

    public abstract DataSource getDataSource();

    public abstract void destroy();

    /**
     * 
     * 
     * @param maxActive
     */
    public void setMaxActive(int maxActive) {
        ((DruidDataSource) this.getDataSource()).setMaxActive(maxActive);
    }

    /**
     * 
     * 
     * @param minIdle
     */
    public void setMinIdle(int minIdle) {
        ((DruidDataSource) this.getDataSource()).setMinIdle(minIdle);
    }

    /**
     * 
     * 
     * @param properties
     */
    public void setConnectionProperties(String properties) {
        ((DruidDataSource) this.getDataSource()).setConnectionProperties(properties);
    }

    /**
     * 
     * @param mergeServerConfig
     */
    public DataSourceHolder(MergeServerConfig mergeServerConfig) {
        this.mergeServerConfig = mergeServerConfig;
    }

    /**
     * 
     * 
     * @param v
     */
    public void audit(double v) {
        lock.lock();
        try {
            auditValue = auditValue * AUDIT_FACTOR + v;
            if (auditValue - AUDIT_THRESHOLD > 0.0) {// 置为不可用状态, 一分钟内某集群ms异常为47次。
                invalidTime = System.currentTimeMillis();
                auditValue = 0;

                if (logger.isInfoEnabled()) {
                    logger.info("invalid cluster " + this.toString() + ", at "
                                + new Date(invalidTime));
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 
     * 
     * @return
     */
    public boolean isInvalid() {
        return (System.currentTimeMillis() - invalidTime) < CLUSTER_INVALID_TIME;
    }

    /**
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return mergeServerConfig.toString();
    }

    /**
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof DataSourceHolder) {
            DataSourceHolder dsh = (DataSourceHolder) obj;
            return this.toString().equals(dsh.toString());
        }

        return false;
    }

}