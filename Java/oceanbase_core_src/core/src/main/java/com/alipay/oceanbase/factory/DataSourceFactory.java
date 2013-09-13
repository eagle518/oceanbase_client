package com.alipay.oceanbase.factory;

import static com.alipay.oceanbase.util.OBDataSourceConstants.DAEMON_TASK_MODULE_LOGGER_NAME;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.alibaba.druid.pool.DruidDataSource;
import com.alipay.oceanbase.config.DruidConfig;
import com.alipay.oceanbase.config.MergeServerConfig;
import com.alipay.oceanbase.util.Helper;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: DruidDataSourceFactory.java, v 0.1 2013-5-24 下午3:59:54 liangjie.li Exp $
 */
public class DataSourceFactory {
    private static final Logger logger = Logger.getLogger(DAEMON_TASK_MODULE_LOGGER_NAME);

    /**
     * 
     * 
     * @param server
     * @param config
     * @return
     * @throws SQLException
     */
    public static DataSourceHolder getHolder(MergeServerConfig msconfig,
                                             Map<String, String> configParams) throws SQLException {
        final DruidDataSource druid = newDataSoruce(msconfig, configParams);

        return new DataSourceHolder(msconfig) {

            /**
             * 
             * @see com.alipay.oceanbase.factory.DataSourceHolder#getDataSource()
             */
            @Override
            public DataSource getDataSource() {
                return druid;
            }

            /**
             * 
             * @see com.alipay.oceanbase.factory.DataSourceHolder#destroy()
             */
            @Override
            public void destroy() {
                druid.close();
            }
        };
    }

    /**
     * 
     * 
     * @param server
     * @param map
     * @return
     * @throws SQLException
     */
    public static DruidDataSource newDataSoruce(MergeServerConfig server, Map<String, String> map)
                                                                                                  throws SQLException {
        DruidDataSource druid = new DruidDataSource();

        if (logger.isInfoEnabled()) {
            logger.info("create druid datasource, param: " + map);
        }

        for (DruidConfig config : DruidConfig.values()) {
            String value = null;
            if (config.name().equals("url")) {
                value = Helper.getMySqlConURL(server.getIp(), server.getPort());
            } else if (config.name().equals("name")) {
                SimpleDateFormat sdf = new SimpleDateFormat("MM_dd HH_mm_ss");

                value = String.format("%s_%s[%s]", server.getIp(), server.getPort(),
                    sdf.format(new Date()));
            } else {
                value = map.get(config.name());
            }
            config.setValue(druid, value);
        }
        druid.init();

        return druid;
    }

}