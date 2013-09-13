package com.alipay.oceanbase.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: ConfigLoader.java, v 0.1 2013-7-8 17:52:06 liangjie.li Exp $
 */
public class ConfigLoader {

    private static final Logger logger = Logger.getLogger(ConfigLoader.class);

    /**
     * 
     * 
     * @param configURL
     * @return
     */
    public static String getDataId(URL configURL) {
        String queryString = configURL.getQuery();
        if (StringUtils.isNotBlank(queryString)) {
            String[] arr = queryString.split("=");
            if (arr != null && arr.length == 2) {
                return arr[1];
            }
        }
        return "";
    }

    /**
     * 
     * 
     * @param configURL
     * @return
     */
    public static Properties load(URL configURL) {

        if (logger.isInfoEnabled()) {
            logger.info("load properties, configUrl: " + configURL.toString());
        }

        int i = 0;
        while (i < FileUtil.RETRY_TIMES) {
            try {
                Properties properties = new Properties();
                properties.load(configURL.openStream());

                // update local cache info
                FileUtil.updateFile(FileUtil.LOCAL_JAR_PATH, getDataId(configURL), configURL);

                return properties;
            } catch (IOException ex) {
                if (logger.isInfoEnabled() && i < FileUtil.RETRY_TIMES - 1) {
                    logger.info("will retry to get config, the times: " + (i + 1));
                } else {
                    if (logger.isInfoEnabled()) {
                        logger.info("load local properties, file: " + FileUtil.LOCAL_JAR_PATH
                                    + getDataId(configURL));
                    }

                    // load local cache info
                    Properties localProperties = new Properties();
                    FileInputStream fis;
                    try {
                        fis = new FileInputStream(new File(FileUtil.LOCAL_JAR_PATH
                                                           + getDataId(configURL)));
                        localProperties.load(fis);
                        fis.close();

                        return localProperties;
                    } catch (FileNotFoundException e) {
                        logger.error("", e);
                        throw new IllegalArgumentException("configurl cannot open, " + configURL);
                    } catch (IOException e) {
                        logger.error("", e);
                        throw new IllegalArgumentException("configurl cannot open, " + configURL);
                    }
                }
                i++;
            }
        }

        return null;
    }

    /**
     * 
     * 
     * @param configUrl
     * @return
     */
    public static Properties load(String configUrl) {
        if (StringUtils.isBlank(configUrl)) {
            throw new IllegalArgumentException("configurl must be not null!");
        }

        URL configURL = null;
        try {
            configURL = new URL(configUrl);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("configurl has error, " + configUrl);
        }

        return load(configURL);
    }

    private ConfigLoader() {
    }

}
