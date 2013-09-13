package com.alipay.oceanbase;

import static com.alipay.oceanbase.util.OBDataSourceConstants.CLUSTER_ADDRESS;
import static com.alipay.oceanbase.util.OBDataSourceConstants.DEFAULT_MYSQL_DRIVER_CLASS;
import static com.alipay.oceanbase.util.OBDataSourceConstants.DS_CONFIG;
import static com.alipay.oceanbase.util.OBDataSourceConstants.PASSWORD;
import static com.alipay.oceanbase.util.OBDataSourceConstants.PERIOD;
import static com.alipay.oceanbase.util.OBDataSourceConstants.USER_NAME;

import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.alipay.oceanbase.config.OBDataSourceConfig;
import com.alipay.oceanbase.group.EquityMSManager;
import com.alipay.oceanbase.group.MergeServerSelector;
import com.alipay.oceanbase.task.DSStatusPrintOutTask;
import com.alipay.oceanbase.task.UpdateConfigTask;
import com.alipay.oceanbase.util.ConfigLoader;
import com.alipay.oceanbase.util.Helper;
import com.alipay.oceanbase.util.ObUtil;
import com.alipay.oceanbase.util.ThreadLocalRandom;
import com.alipay.oceanbase.util.log.CommonLoggerComponent;
import com.alipay.oceanbase.util.thread.CustomerThreadFactory;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: OBGroupDataSource.java, v 0.1 2013-5-24 下午4:13:34 liangjie.li Exp $
 */
public class OBGroupDataSource implements DataSource {

    private static final Logger                  logger               = Logger
                                                                          .getLogger(OBGroupDataSource.class);

    private AtomicBoolean                        inited               = new AtomicBoolean(false);
    private AtomicBoolean                        close                = new AtomicBoolean(false);
    private AtomicReference<OBDataSourceConfig>  config               = new AtomicReference<OBDataSourceConfig>();
    private AtomicReference<MergeServerSelector> mergeServerSelectory = new AtomicReference<MergeServerSelector>();

    private Map<String, String>                  configParams         = new HashMap<String, String>();
    private String                               configURL            = null;
    private int                                  period               = -1;
    protected int                                isStrongConsistency  = -1;

    protected final ScheduledExecutorService     scheduler            = Executors
                                                                          .newScheduledThreadPool(
                                                                              2,
                                                                              new CustomerThreadFactory());

    /**
     * 
     * 
     * @throws RemoteConfigurationException 
     * @throws MalformedURLException 
     * @throws Exception
     */
    public void init() throws SQLException {
        if (!inited.get()) {
            if (logger.isInfoEnabled()) {
                logger.info("datasource init ...");
            }

            inited.set(true);

            CommonLoggerComponent.init();
            try {
                Class.forName(DEFAULT_MYSQL_DRIVER_CLASS);
            } catch (Exception e) {
                logger.error("", e);
            }

            String userName = this.configParams.get(USER_NAME);
            String password = this.configParams.get(PASSWORD);
            String clusterAddress = this.configParams.get(CLUSTER_ADDRESS);

            Properties properties = ConfigLoader.load(this.configURL);

            // check connectionProperties 
            Map<String, String> connectionPropertiesMap = Helper
                .loadDsConnectionProperties(properties.getProperty(DS_CONFIG));
            if (connectionPropertiesMap != null && !connectionPropertiesMap.isEmpty()) {
                this.configParams.putAll(connectionPropertiesMap);
            }

            if (StringUtils.isBlank(clusterAddress)) {// for 1.0.0
                clusterAddress = properties.getProperty(CLUSTER_ADDRESS);
            }

            if (logger.isInfoEnabled()) {
                logger.info("datasource parameters: " + this.configParams);
            }

            config
                .set(new OBDataSourceConfig(userName, password, clusterAddress, this.configParams));
            isStrongConsistency = ObUtil.isConsistency(userName, password, clusterAddress);

            UpdateConfigTask task = new UpdateConfigTask(userName, password, configURL,
                this.config.get(), this.configParams, this);
            mergeServerSelectory.set(new EquityMSManager(config.get(), task));

            String val = this.configParams.get(PERIOD);
            if (StringUtils.isNotBlank(val)) {
                period = Integer.parseInt(val);
            }
            if (period <= 0) {
                period = ThreadLocalRandom.current().nextInt(30) + 30;// 30-60s
            }
            scheduler.scheduleAtFixedRate(task, period, period, TimeUnit.SECONDS);
            if (logger.isInfoEnabled()) {
                logger.info("update config task scheduling period:" + period + "s");
            }

            scheduler.scheduleAtFixedRate(new DSStatusPrintOutTask(this), 0, 30, TimeUnit.SECONDS);
            if (logger.isInfoEnabled()) {
                logger.info("datasource status print task scheduling period:" + 30 + "s");
                logger.info("datasource init end ...");
            }
        }
    }

    /**
     * 
     */
    public void destroy() {
        if (inited.get()) {
            this.close.set(true);
            this.inited.set(false);

            this.config.get().destroyAllDruidDS();
            this.scheduler.shutdown();
        }
    }

    /**
     * 
     */
    protected void checkClose() {
        if (this.close.get()) {
            throw new IllegalStateException("ob datasource have close");
        }
    }

    // /////////////////////// getter and setter ///////////////////////
    public void setConfigURL(String url) {
        if (StringUtils.isNotBlank(url)) {
            this.configURL = url;
        } else {
            throw new IllegalArgumentException("config url is null");
        }
    }

    public void setUsername(String username) {
        this.configParams.put(USER_NAME, username);
    }

    public void setPassword(String password) {
        this.configParams.put(PASSWORD, password);
    }

    public void setLMS(String clusterAddress) {
        this.configParams.put(CLUSTER_ADDRESS, clusterAddress);
    }

    public MergeServerSelector getDBSelector() {
        return this.mergeServerSelectory.get();
    }

    public void setDBSelector(MergeServerSelector dbSelector) {
        this.mergeServerSelectory.set(dbSelector);
    }

    public void setConfig(OBDataSourceConfig config) {
        this.config.set(config);
    }

    public OBDataSourceConfig getConfig() {
        return this.config.get();
    }

    public void setDataSourceConfig(Map<String, String> dataSourceConfig) {// validate config of the datasource
        if (StringUtils.isBlank(dataSourceConfig.get("username"))) {
            throw new IllegalArgumentException("username must provide");
        }
        if (StringUtils.isBlank(dataSourceConfig.get("password"))) {
            throw new IllegalArgumentException("password must provide");
        }
        this.configParams = dataSourceConfig;
    }

    // /////////////////////// implement methods ///////////////////////
    /**
     * 
     * @see javax.sql.DataSource#getConnection()
     */
    public TGroupConnection getConnection() throws SQLException {
        this.checkClose();
        return new TGroupConnection(this);
    }

    /**
     * 
     * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
     */
    public TGroupConnection getConnection(String username, String password) throws SQLException {
        this.checkClose();
        return new TGroupConnection(this, username, password);
    }

    private PrintWriter out = null;

    /**
     * 
     * @see javax.sql.CommonDataSource#getLogWriter()
     */
    public PrintWriter getLogWriter() throws SQLException {
        this.checkClose();
        return out;
    }

    /**
     * 
     * @see javax.sql.CommonDataSource#setLogWriter(java.io.PrintWriter)
     */
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.checkClose();
        this.out = out;
    }

    private int seconds = 0;

    /**
     * 
     * @see javax.sql.CommonDataSource#getLoginTimeout()
     */
    public int getLoginTimeout() throws SQLException {
        this.checkClose();
        return seconds;
    }

    /**
     * 
     * @see javax.sql.CommonDataSource#setLoginTimeout(int)
     */
    public void setLoginTimeout(int seconds) throws SQLException {
        this.checkClose();
        this.seconds = seconds;
    }

    /**
     * 
     * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        this.checkClose();
        return this.getClass().isAssignableFrom(iface);
    }

    /**
     * 
     * @see java.sql.Wrapper#unwrap(java.lang.Class)
     */
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        this.checkClose();
        try {
            return (T) this;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
