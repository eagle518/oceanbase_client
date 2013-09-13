package com.alipay.oceanbase;

import static com.alipay.oceanbase.RemoteConfigs.CLUSTER_ADDRESS;
import static com.alipay.oceanbase.RemoteConfigs.PASSWORD;
import static com.alipay.oceanbase.RemoteConfigs.USER_NAME;
import static com.alipay.oceanbase.log.CommonLoggerComponent.PORXY_MODULE_LOGGER_NAME;

import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.alipay.oceanbase.log.CommonLoggerComponent;
import com.alipay.oceanbase.util.CustomerThreadFactory;
import com.alipay.oceanbase.util.InnerTableOperator;
import com.alipay.oceanbase.util.SecureIdentityLoginModule;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: OceanbaseDataSourceProxy.java, v 0.1 2013-5-24 上午11:21:22 liangjie.li Exp $
 */
public class OceanbaseDataSourceProxy implements DataSource {

    private static final Logger              logger                = Logger
                                                                       .getLogger(PORXY_MODULE_LOGGER_NAME);

    private static AutoUpdater               autoUpdater           = null;
    private static RemoteConfigs             remoteConfigs         = null;

    public int                               delayUnloadTime       = 5 * 60 * 1000;
    protected final int                      CHECK_UPDATE_INTERVAL = 1;
    protected URL                            configURL             = null;
    protected volatile DataSource            realDataSource        = null;

    private final AtomicBoolean              inited                = new AtomicBoolean(false);
    protected Map<String, String>            configurations        = new HashMap<String, String>();
    protected final ScheduledExecutorService scheduler             = Executors
                                                                       .newScheduledThreadPool(
                                                                           1,
                                                                           new CustomerThreadFactory());

    /**
     * 
     * 
     * @throws Exception
     */
    public void init() throws Exception {
        if (!inited.get()) {
            inited.set(true);

            if (logger.isInfoEnabled()) {
                logger.info("OceanbaseDataSourceProxy init start");
            }

            checkParameters();

            remoteConfigs = RemoteConfigs.getInstance(configURL);

            // if there is username&password in the remote configs, we'll use.
            if (StringUtils.isNotBlank(remoteConfigs.getUserName())) {
                this.configurations.put(USER_NAME, remoteConfigs.getUserName());
            }
            if (StringUtils.isNotBlank(remoteConfigs.getPassword())) {
                this.configurations.put(PASSWORD, remoteConfigs.getPassword());
            }
            // decode password
            if (StringUtils.isNotBlank(this.configurations.get("password"))) {
                this.configurations.put(PASSWORD,
                    SecureIdentityLoginModule.decode(this.configurations.get("password")));
            }
            this.configurations.put(CLUSTER_ADDRESS, remoteConfigs.getClusterAddress());

            this.realDataSource = Loader.loadCoreJar(remoteConfigs.getCoreJarPath(),
                remoteConfigs.getCoreJarVersion(), remoteConfigs.getMD5(),
                this.configURL.toString(), this.configurations);

            InnerTableOperator.reportCoreJarVersion(realDataSource,
                remoteConfigs.getCoreJarVersion());

            enableAutoUpdaterDaemonThread();
            CommonLoggerComponent.init();
            if (logger.isInfoEnabled()) {
                logger.info("OceanbaseDataSourceProxy init end");
            }
        } else {
            logger.error("OceanbaseDataSourceProxy has inited");
        }
    }

    /**
     * 
     * 
     * @throws Exception
     */
    public void destroy() throws Exception {
        if (this.inited.get()) {
            this.inited.set(false);

            if (logger.isInfoEnabled()) {
                logger.info("unload oceanbase datasource");
            }

            this.destroyRealDataSource();
            this.scheduler.shutdown();

            if (logger.isInfoEnabled()) {
                logger.info("Oceanbase Datasource Autoupdate Thread has shutdown");
            }

        }
    }

    /**
     * 
     */
    private void enableAutoUpdaterDaemonThread() {
        autoUpdater = new AutoUpdater(this, remoteConfigs.getPercentage(),
            remoteConfigs.getCoreJarVersion(), delayUnloadTime);
        scheduler.scheduleAtFixedRate(autoUpdater, CHECK_UPDATE_INTERVAL, CHECK_UPDATE_INTERVAL,
            TimeUnit.MINUTES);

        if (logger.isInfoEnabled()) {
            logger.info("auto update task scheduling period: " + CHECK_UPDATE_INTERVAL);
        }
    }

    /**
     * 
     * 
     * @throws Exception
     */
    protected void destroyRealDataSource() throws Exception {
        Loader.unLoad(this.realDataSource);
        this.realDataSource = null;

        if (logger.isInfoEnabled()) {
            logger.info("unload complete");
        }
    }

    private void checkParameters() {
        if (this.configURL == null) {
            throw new IllegalArgumentException("please add configURL");
        }
    }

    //////////////////////////////// setter and getter //////////////////////////////
    public void setUsername(String username) {
        this.configurations.put("username", username);
    }

    public void setPassword(String password) {
        this.configurations.put("password", password);
    }

    public void setConnectionProperties(String properties) {
        this.configurations.put("connectionProperties", properties);
    }

    public void setInitialSize(int initialSize) {
        this.configurations.put("initialSize", String.valueOf(initialSize));
    }

    public void setMinIdle(int minIdle) {
        this.configurations.put("minIdle", String.valueOf(minIdle));
    }

    public void setMaxActive(int maxActive) {
        this.configurations.put("maxActive", String.valueOf(maxActive));
    }

    public void setMaxWait(long maxWait) {
        this.configurations.put("maxWait", String.valueOf(maxWait));
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        this.configurations.put("testOnBorrow", String.valueOf(testOnBorrow));
    }

    public void setTestOnReturn(boolean testOnReturn) {
        this.configurations.put("testOnReturn", String.valueOf(testOnReturn));
    }

    public void setTestWhileIdle(boolean testWhileIdle) {
        this.configurations.put("testWhileIdle", String.valueOf(testWhileIdle));
    }

    public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis) {
        this.configurations.put("timeBetweenEvictionRunsMillis",
            String.valueOf(timeBetweenEvictionRunsMillis));
    }

    public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis) {
        this.configurations.put("minEvictableIdleTimeMillis",
            String.valueOf(minEvictableIdleTimeMillis));
    }

    public void setQueryTimeout(int queryTimeout) {
        this.configurations.put("queryTimeout", String.valueOf(queryTimeout));
    }

    public void setValidationQueryTimeout(int validationQueryTimeout) {
        this.configurations.put("validationQueryTimeout", String.valueOf(validationQueryTimeout));
    }

    public void setValidationQuery(String validationQuery) {
        this.configurations.put("validationQuery", validationQuery);
    }

    public void setDefaultTransactionIsolation(boolean defaultTransactionIsolation) {
        this.configurations.put("defaultTransactionIsolation",
            String.valueOf(defaultTransactionIsolation));
    }

    public void setDefaultReadOnly(boolean defaultReadOnly) {
        this.configurations.put("defaultReadOnly", String.valueOf(defaultReadOnly));
    }

    public void setDefaultAutoCommit(boolean defaultAutoCommit) {
        this.configurations.put("defaultAutoCommit", String.valueOf(defaultAutoCommit));
    }

    public void setConnectionErrorRetryAttempts(int connectionErrorRetryAttempts) {
        this.configurations.put("connectionErrorRetryAttempts",
            String.valueOf(connectionErrorRetryAttempts));
    }

    public void setMaxWaitThreadCount(int maxWaitThreadCount) {
        this.configurations.put("maxWaitThreadCount", String.valueOf(maxWaitThreadCount));
    }

    public void setMaxPoolPreparedStatementPerConnectionSize(int maxPoolPreparedStatementPerConnectionSize) {
        this.configurations.put("maxPoolPreparedStatementPerConnectionSize",
            String.valueOf(maxPoolPreparedStatementPerConnectionSize));
    }

    public void setRemoveAbandonedTimeout(long removeAbandonedTimeout) {
        this.configurations.put("removeAbandonedTimeout", String.valueOf(removeAbandonedTimeout));
    }

    public void setBreakAfterAcquireFailure(boolean breakAfterAcquireFailure) {
        this.configurations.put("breakAfterAcquireFailure",
            String.valueOf(breakAfterAcquireFailure));
    }

    public void setConfigURL(String configURL) {
        try {
            this.configURL = new URL(configURL);
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException(String.format("please check your configURL [%s]",
                configURL), ex);
        }
    }

    public void setPeriod(int period) {
        this.configurations.put("period", String.valueOf(period));
    }

    public Map<String, String> getConfigurations() {
        return configurations;
    }

    public void setLevel(String level) {
        CommonLoggerComponent.setLevel(level);
    }

    public void setDelayUnloadTime(int delayUnloadTime) {
        this.delayUnloadTime = delayUnloadTime;
    }

    //////////////////////////////// implement javax.sql.DataSource //////////////////////////////
    @Override
    public Connection getConnection() throws SQLException {
        return this.realDataSource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return this.realDataSource.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return this.realDataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.realDataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.realDataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return this.realDataSource.getLoginTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.realDataSource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.realDataSource.isWrapperFor(iface);
    }

}
