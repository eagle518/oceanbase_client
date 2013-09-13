package com.alipay.oceanbase.group;

import java.sql.SQLException;

import com.alipay.oceanbase.config.ClusterConfig;
import com.alipay.oceanbase.factory.DataSourceHolder;
import com.alipay.oceanbase.util.parse.SqlHintType;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: DBSelector.java, v 0.1 2013-6-3 下午5:59:45 liangjie.li Exp $
 */
public interface MergeServerSelector {

    <T> T tryExecute(DataSourceTryer<T> tryer, boolean isConsistency, SqlHintType isMasterCluster,
                     Object... args) throws SQLException;

    public void setReadDistTable(ClusterConfig[] readDistTable);

    public static interface DataSourceTryer<T> {
        T tryOnDataSource(DataSourceHolder dsw, Object... args) throws SQLException;
    }

}
