package com.alipay.oceanbase;

import static com.alipay.oceanbase.log.CommonLoggerComponent.DAEMON_TASK_MODULE_LOGGER_NAME;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.alipay.oceanbase.rule.AndPredicate;
import com.alipay.oceanbase.rule.BooleanPredicate;
import com.alipay.oceanbase.rule.HostAddressPredicate;
import com.alipay.oceanbase.rule.OrPredicate;
import com.alipay.oceanbase.rule.PercentagePredicate;
import com.alipay.oceanbase.rule.Predicate;
import com.alipay.oceanbase.util.InnerTableOperator;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: AutoUpdater.java, v 0.1 2013-5-24 下午1:43:14 liangjie.li Exp $
 */
public class AutoUpdater implements Runnable {

    private final OceanbaseDataSourceProxy proxy;

    private long                           newestLoadTime         = System.currentTimeMillis();

    private boolean                        needUnload             = false;
    private DataSource                     currentDataSource      = null;

    private String                         rollbackCoreJarVersion = "";
    private String                         newestCoreJarVersion   = "";
    private int                            newestPercentage       = 0;
    private int                            delayUnloadTime        = 0;
    private RemoteConfigs                  remoteConfigs          = null;

    private static final Logger            log                    = Logger
                                                                      .getLogger(DAEMON_TASK_MODULE_LOGGER_NAME);

    protected AutoUpdater(OceanbaseDataSourceProxy proxy, int percentage, String coreJarVersion,
                          int delayUnloadTime) {
        this.proxy = proxy;
        this.currentDataSource = proxy.realDataSource;
        this.newestPercentage = percentage;
        this.newestCoreJarVersion = coreJarVersion;
        this.delayUnloadTime = delayUnloadTime;
    }

    @Override
    public void run() {
        try {
            this.remoteConfigs = RemoteConfigs.getInstance(proxy.configURL);
            loadOrIgnoreRemoteCoreJar();
        } catch (Throwable ex) {
            log.warn("auto updater fail", ex);
        }
    }

    /**
     * 
     * 
     * @throws Throwable
     */
    private void loadOrIgnoreRemoteCoreJar() throws Throwable {
        if (needUnLoad()) {
            Loader.unLoad(currentDataSource);
            updateCurrentDataSourcePointer(proxy);
            needUnload = false;

            if (log.isInfoEnabled()) {
                log.info("unload the previous version end");
            }
        }
        if (needUpdate()) {
            if (updateInDelayUnLoadTime()) {
                this.rollback();
                return;
            } else {
                this.upgrade();
            }
        }
        this.newestPercentage = this.remoteConfigs.getPercentage();
    }

    private boolean updateInDelayUnLoadTime() {
        long current = System.currentTimeMillis();
        return current - newestLoadTime < delayUnloadTime;
    }

    private void updateCurrentDataSourcePointer(OceanbaseDataSourceProxy proxy) {
        this.currentDataSource = proxy.realDataSource;
    }

    private boolean needUnLoad() {
        long time = System.currentTimeMillis();
        return needUnload && (delayUnloadTime < time - newestLoadTime);
    }

    private void rollback() throws Exception {
        if (StringUtils.isBlank(this.rollbackCoreJarVersion)) {
            if (log.isInfoEnabled()) {
                log.info("not find rollback version,not allow rollback");
            }

            Thread.sleep(delayUnloadTime);
            return;
        }

        if (log.isInfoEnabled()) {
            log.info("oceanbase datasource start rollback");
        }

        DataSource tmp = proxy.realDataSource;
        proxy.realDataSource = this.currentDataSource;
        Loader.unLoad(tmp);

        needUnload = false;
        this.newestCoreJarVersion = this.rollbackCoreJarVersion;
        this.rollbackCoreJarVersion = "";
        InnerTableOperator.reportCoreJarVersion(currentDataSource, newestCoreJarVersion);

        if (log.isInfoEnabled()) {
            log.info("oceanbase datasource rollback end");
        }

        Thread.sleep(delayUnloadTime);
    }

    private void upgrade() throws Exception {
        if (log.isInfoEnabled()) {
            log.info("oceanbase datasource start upgrade");
        }

        DataSource ds = Loader.loadCoreJar(this.remoteConfigs.getCoreJarPath(),
            this.remoteConfigs.getCoreJarVersion(), this.remoteConfigs.getMD5(),
            proxy.configURL.toString(), proxy.getConfigurations());
        this.proxy.realDataSource = ds;

        this.rollbackCoreJarVersion = this.newestCoreJarVersion;
        this.newestCoreJarVersion = this.remoteConfigs.getCoreJarVersion();
        newestLoadTime = System.currentTimeMillis();
        needUnload = true;

        InnerTableOperator.reportCoreJarVersion(proxy.realDataSource, this.newestCoreJarVersion);

        if (log.isInfoEnabled()) {
            log.info("oceanbase datasource upgrade end");
        }
    }

    private boolean needUpdate() {
        boolean isCoreVersionChange = (!this.newestCoreJarVersion.equals(this.remoteConfigs
            .getCoreJarVersion()));
        boolean isAll = (100 == this.remoteConfigs.getPercentage());
        boolean isChange = (this.newestPercentage != this.remoteConfigs.getPercentage());

        Predicate enableUpdate = new BooleanPredicate(this.remoteConfigs.isEnableUpdate());
        Predicate coreJarVersionChange = new BooleanPredicate(isCoreVersionChange);
        Predicate predicate1 = new AndPredicate(enableUpdate, coreJarVersionChange);
        Predicate whiteList = new HostAddressPredicate(this.remoteConfigs.getWhiteList());
        Predicate all = new BooleanPredicate(isAll);
        Predicate percentageChange = new BooleanPredicate(isChange);

        Predicate percentage = new PercentagePredicate(this.remoteConfigs.getPercentage());
        Predicate predicate2 = new OrPredicate(whiteList, new OrPredicate(all, new AndPredicate(
            percentageChange, percentage)));

        Predicate predicate = new AndPredicate(predicate1, predicate2);
        return predicate.needUpdate();
    }
}