package com.alipay.oceanbase.log;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

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

    public static final String   COMMON_LOGGER_NAME             = "com.alipay.oceanbase";
    public static final String   PORXY_MODULE_LOGGER_NAME       = "porxyModuleLogger";
    public static final String   DAEMON_TASK_MODULE_LOGGER_NAME = "daemonTaskModuleLogger";

    public static Level          level                          = Level.INFO;

    private static final Logger  logger                         = Logger
                                                                    .getLogger(PORXY_MODULE_LOGGER_NAME);
    public static final Logger   PORXY_MODULE_LOGGER            = Logger
                                                                    .getLogger(PORXY_MODULE_LOGGER_NAME);
    public static final Logger   DAEMON_TASK_MODULE_LOGGER      = Logger
                                                                    .getLogger(DAEMON_TASK_MODULE_LOGGER_NAME);

    private static AtomicBoolean isStarted                      = new AtomicBoolean(false);

    private CommonLoggerComponent() {
    }

    public static void init() {
        if (isStarted.compareAndSet(false, true)) {
            initLog();

            if (logger.isInfoEnabled()) {
                logger.info("obdatasource logger init end!");
            }
        }
    }

    static void initLog() {
        Appender proxyModuleMonitorAppender = buildAppender("proxyModuleMonitorAppender",
            "obdatasource-proxy-monitor.log", "%d{ISO8601} [%t] %-5p %C{1}(%L): %m%n",
            "'.'yyyy-MM-dd");
        PORXY_MODULE_LOGGER.setAdditivity(false);
        PORXY_MODULE_LOGGER.removeAllAppenders();
        PORXY_MODULE_LOGGER.addAppender(proxyModuleMonitorAppender);
        PORXY_MODULE_LOGGER.setLevel(level);

        Appender daemonTaskMonitorAppender = buildAppender("daemonTaskMonitorAppender",
            "obdatasource-task-monitor.log", "%d{ISO8601} [%t] %-5p %C{1}(%L): %m%n",
            "'.'yyyy-MM-dd");
        DAEMON_TASK_MODULE_LOGGER.setAdditivity(false);
        DAEMON_TASK_MODULE_LOGGER.removeAllAppenders();
        DAEMON_TASK_MODULE_LOGGER.addAppender(daemonTaskMonitorAppender);
        DAEMON_TASK_MODULE_LOGGER.setLevel(level);
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
        if (userHome != null) {
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
