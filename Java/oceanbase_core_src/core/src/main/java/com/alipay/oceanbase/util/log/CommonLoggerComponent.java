package com.alipay.oceanbase.util.log;

import static com.alipay.oceanbase.util.OBDataSourceConstants.COMMON_LOGGER_NAME;
import static com.alipay.oceanbase.util.OBDataSourceConstants.DAEMON_TASK_MODULE_LOGGER_NAME;
import static com.alipay.oceanbase.util.OBDataSourceConstants.DS_STATUS_MODULE_LOGGER_NAME;
import static com.alipay.oceanbase.util.OBDataSourceConstants.LB_MODULE_LOGGER_NAME;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * 
 * 
 * @author liangjieli
 * @version $Id: CommonLoggerComponent.java, v 0.1 Jun 9, 2013 11:05:47 AM liangjieli Exp $
 */
public class CommonLoggerComponent {

    public static Level          level                     = Level.INFO;

    private static final Logger  logger                    = Logger
                                                               .getLogger(CommonLoggerComponent.class);
    public static final Logger   COMMON_LOGGER             = Logger.getLogger(COMMON_LOGGER_NAME);
    public static final Logger   LB_MODULE_LOGGER          = Logger
                                                               .getLogger(LB_MODULE_LOGGER_NAME);
    public static final Logger   DAEMON_TASK_MODULE_LOGGER = Logger
                                                               .getLogger(DAEMON_TASK_MODULE_LOGGER_NAME);
    public static final Logger   DS_STATUS_MODULE_LOGGER   = Logger
                                                               .getLogger(DS_STATUS_MODULE_LOGGER_NAME);

    private static AtomicBoolean isStarted                 = new AtomicBoolean(false);

    private CommonLoggerComponent() {
    }

    public static void init() {
        if (isStarted.compareAndSet(false, true)) {
            initLog();

            if (logger.isInfoEnabled()) {
                logger.info("logger component init end!");
            }
        }
    }

    static void initLog() {
        Appender commonMonitorAppender = buildAppender("commonMonitorAppender", "obdatasource.log",
            "%d{ISO8601} [%t] %-5p %C{1}(%L): %m%n", "'.'yyyy-MM-dd");
        COMMON_LOGGER.setAdditivity(false);
        COMMON_LOGGER.removeAllAppenders();
        COMMON_LOGGER.addAppender(commonMonitorAppender);
        COMMON_LOGGER.setLevel(level);

        Appender lbModuleMonitorAppender = buildAppender("lbModuleMonitorAppender",
            "obdatasource-lb-monitor.log", "%d{ISO8601} [%t] %-5p %C{1}(%L): %m%n", "'.'yyyy-MM-dd");
        LB_MODULE_LOGGER.setAdditivity(false);
        LB_MODULE_LOGGER.removeAllAppenders();
        LB_MODULE_LOGGER.addAppender(lbModuleMonitorAppender);
        LB_MODULE_LOGGER.setLevel(level);

        Appender daemonTaskMonitorAppender = buildAppender("daemonTaskMonitorAppender",
            "obdatasource-task-monitor.log", "%d{ISO8601} [%t] %-5p %C{1}(%L): %m%n",
            "'.'yyyy-MM-dd");
        DAEMON_TASK_MODULE_LOGGER.setAdditivity(false);
        DAEMON_TASK_MODULE_LOGGER.removeAllAppenders();
        DAEMON_TASK_MODULE_LOGGER.addAppender(daemonTaskMonitorAppender);
        DAEMON_TASK_MODULE_LOGGER.setLevel(level);

        Appender dsStatusMonitorAppender = buildAppender("dsStatusMonitorAppender",
            "obdatasource-ds-monitor.log", "%d{yyyy-MM-dd HH:mm:ss}, %m%n", "'.'yyyy-MM-dd_HH");
        DS_STATUS_MODULE_LOGGER.setAdditivity(false);
        DS_STATUS_MODULE_LOGGER.removeAllAppenders();
        DS_STATUS_MODULE_LOGGER.addAppender(dsStatusMonitorAppender);
        DS_STATUS_MODULE_LOGGER.setLevel(level);
    }

    /**
     * 
     * 
     * @param level
     */
    public static void setLevel(String level) {
        if ("debug".equalsIgnoreCase(level.trim())) {
            CommonLoggerComponent.level = Level.DEBUG;
        } else if ("warn".equalsIgnoreCase(level.trim())) {
            CommonLoggerComponent.level = Level.WARN;
        } else if ("error".equalsIgnoreCase(level.trim())) {
            CommonLoggerComponent.level = Level.ERROR;
        } else if ("off".equalsIgnoreCase(level.trim())) {
            CommonLoggerComponent.level = Level.OFF;
        } else if ("trace".equalsIgnoreCase(level.trim())) {
            CommonLoggerComponent.level = Level.TRACE;
        } else if ("all".equalsIgnoreCase(level.trim())) {
            CommonLoggerComponent.level = Level.ALL;
        } else if ("fatal".equalsIgnoreCase(level.trim())) {
            CommonLoggerComponent.level = Level.FATAL;
        }
    }

    /**
     * 
     * 
     * @param name
     * @param fileName
     * @param pattern
     * @param dataPattern
     * @return
     */
    private static Appender buildAppender(String name, String fileName, String pattern,
                                          String dataPattern) {
        DailyRollingFileAppender appender = new DailyRollingFileAppender();
        appender.setName(name);
        appender.setAppend(true);
        appender.setEncoding("GBK");
        appender.setLayout(new PatternLayout(pattern));
        appender.setDatePattern(dataPattern);
        appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());
        appender.activateOptions();
        return appender;
    }

    /**
     * 
     * 
     * @return
     */
    private static String getLogPath() {
        String userHome = System.getProperty("user.home");
        if (StringUtils.isNotBlank(userHome)) {
            if (!userHome.endsWith(File.separator)) {
                userHome += File.separator;
            }
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("user.home not found");
            }
        }
        String path = userHome + "logs" + File.separator + "obdatasource" + File.separator;
        File file = new File(path);
        if (!file.exists()) {
            file.mkdirs();
        }
        return path;
    }

}
