package com.alipay.oceanbase.config;

import static com.alipay.oceanbase.util.OBDataSourceConstants.CHECK_VALID_CONNECTION_SQL;
import static com.alipay.oceanbase.util.OBDataSourceConstants.CONNECTION_PROPERTIES;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alipay.oceanbase.util.Helper;

/**
 * 具体见 https://github.com/alibaba/druid/wiki/配置_DruidDataSource参考配置
 * 
 * @author liangjie.li
 * @version $Id: DruidConfig.java, v 0.1 Jun 6, 2013 12:37:23 PM liangjie.li Exp $
 */
public enum DruidConfig {

    name() {// OB自动设置这个数据源名称为后端MergeServer IP
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            druid.setName(value);
        }
    },
    url() {// jdbc的url
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            if (StringUtils.isBlank(value)) {
                throw new IllegalArgumentException("url can't be blank");
            }
            druid.setUrl(value);
        }
    },
    username() {// 用户名
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            if (StringUtils.isBlank(value)) {
                throw new IllegalArgumentException("username can't be blank!");
            } else {
                druid.setUsername(value);
            }
        }
    },
    password() {// 密码
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            if (StringUtils.isBlank(value)) {
                throw new IllegalArgumentException("password can't be blank!");
            }
            druid.setPassword(value);
        }
    },
    connectionProperties() {// 连接的属性,为了避免spring嵌套配置,简化为[p=v;p=v]格式
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            if (StringUtils.isNotBlank(value)) {
                druid.setConnectionProperties(value);
            } else {
                druid.setConnectionProperties(CONNECTION_PROPERTIES);
            }
        }
    },
    initialSize() {// initialSize
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            try {
                if (StringUtils.isNotBlank(value)) {
                    druid.setInitialSize(Integer.valueOf(value));
                }
            } catch (Exception e) {
                throw new IllegalStateException("initialSize must to be numeric", e);
            }
        }
    },
    minIdle() {// 连接池持有的最小连接数, default: 1
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            druid.setMinIdle(StringUtils.isBlank(value) ? 1 : Integer.valueOf(value));
        }
    },
    maxActive() {// 连接池持有的最大连接数, default: 20
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            druid.setMaxActive(StringUtils.isBlank(value) ? 20 : Integer.valueOf(value));
        }
    },
    maxWait() {// 等待可用连接的最大时间(毫秒), default: 1000ms
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            druid.setMaxWait(StringUtils.isBlank(value) ? 1000 : Integer.valueOf(value));
        }
    },
    testWhileIdle() {// 申请连接时检测,如果空闲时间大于timeBetweenEvictionRunsMillis,执行validationQuery检测连接是否有效
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            druid.setTestWhileIdle(StringUtils.isBlank(value) ? true : Boolean.valueOf(value));
        }
    },
    testOnBorrow() {// 验证连接有效性(每次取出时)
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            druid.setTestOnBorrow(StringUtils.isBlank(value) ? false : Boolean.valueOf(value));
        }
    },
    testOnReturn() {// 验证连接有效性(每次把连接还回触发)
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            druid.setTestOnReturn(StringUtils.isBlank(value) ? false : Boolean.valueOf(value));
        }
    },
    timeBetweenEvictionRunsMillis() {// 检测是否有连接到达空闲回收时间（单位：毫秒） 
        /**
         * 
         */
        @Override
        public void setValue(DruidDataSource druid, String value) throws SQLException {
            druid.setTimeBetweenEvictionRunsMillis(StringUtils.isBlank(value) ? 60000L : Integer
                .valueOf(value));
        }
    },
    minEvictableIdleTimeMillis() {// 回收空闲时间（单位：毫秒）
        /**
         * 
         * @see com.alipay.oceanbase.config.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) throws SQLException {
            druid.setMinEvictableIdleTimeMillis(StringUtils.isBlank(value) ? 1000L * 60L * 30L
                : Integer.valueOf(value));
        }
    },
    queryTimeout() {// 查询超时(单位秒)
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            druid.setQueryTimeout(StringUtils.isBlank(value) ? 0 : Integer.valueOf(value));
        }
    },
    validationQueryTimeout() {
        /**
         * 
         * @see com.alipay.oceanbase.config.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) throws SQLException {
            druid.setValidationQueryTimeout(StringUtils.isBlank(value) ? -1 : Integer
                .valueOf(value));
        }
    },
    validationQuery() {// 验证连接有效的SQL,客户端固定不可设置
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            druid.setValidationQuery(CHECK_VALID_CONNECTION_SQL);
        }
    },
    maxPoolPreparedStatementPerConnectionSize() {// 每一个连接缓存的重用的prepare statement数量
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            druid.setMaxOpenPreparedStatements(StringUtils.isBlank(value) ? 50 : Integer
                .valueOf(value));
        }
    },
    connectionInitSqls() {// 创建新连接时回调的SQL,OB用于汇报客户端dirver驱动版本
        /**
         * 
         * @see com.alipay.oceanbase.util.DruidConfig#setValue(com.alibaba.druid.pool.DruidDataSource, java.lang.String)
         */
        @Override
        public void setValue(DruidDataSource druid, String value) {
            List<Object> sql = new ArrayList<Object>(1);
            sql.add(VERSION_REPORT_SQL);
            druid.setConnectionInitSqls(sql);
        }
    };

    public abstract void setValue(DruidDataSource druid, String value) throws SQLException;

    static final String VERSION_REPORT_SQL = Helper.getVersionReportSQL();
}
