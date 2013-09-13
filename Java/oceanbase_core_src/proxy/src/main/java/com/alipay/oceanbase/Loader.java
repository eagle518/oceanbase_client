package com.alipay.oceanbase;

import static com.alipay.oceanbase.log.CommonLoggerComponent.PORXY_MODULE_LOGGER_NAME;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.alipay.oceanbase.exception.ParameterException;
import com.alipay.oceanbase.exception.RemoteConfigurationException;
import com.alipay.oceanbase.util.FileUtil;
import com.alipay.oceanbase.util.MD5Calculator;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: Loader.java, v 0.1 2013-5-24 上午11:43:52 liangjie.li Exp $
 */
public class Loader {

    private static final Logger logger                     = Logger
                                                               .getLogger(PORXY_MODULE_LOGGER_NAME);

    private static final String GROUP_DATASOURCE_CLASS     = "com.alipay.oceanbase.OBGroupDataSource";
    private static final String SETCONFIGURL_METHOD        = "setConfigURL";
    private static final String SETDATASOURCECONFIG_METHOD = "setDataSourceConfig";
    private static final String INIT_METHOD                = "init";

    /**
     * 
     * 
     * @param dataSource
     * @throws Exception
     */
    public static void unLoad(DataSource dataSource) throws Exception {
        if (dataSource != null) {
            Class<?> dataSourceClass = dataSource.getClass();
            Method closeMethod = dataSourceClass.getMethod("destroy", (Class[]) null);
            closeMethod.invoke(dataSource, (Object[]) null);
        }
    }

    /**
     * 
     * 
     * @param coreJarUrl
     * @param configUrl
     * @param configMap
     * @return
     * @throws ClassNotFoundException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws NoSuchMethodException 
     * @throws SecurityException 
     * @throws InvocationTargetException 
     * @throws IllegalArgumentException 
     * @throws Exception
     */
    private static DataSource load(URL coreJarUrl, String configUrl, Map<String, String> configMap)
                                                                                                   throws ClassNotFoundException,
                                                                                                   InstantiationException,
                                                                                                   IllegalAccessException,
                                                                                                   SecurityException,
                                                                                                   NoSuchMethodException,
                                                                                                   IllegalArgumentException,
                                                                                                   InvocationTargetException {
        ClassLoader loader = new URLClassLoader(new URL[] { coreJarUrl },
            Loader.class.getClassLoader());

        Class<?> a = loader.loadClass(GROUP_DATASOURCE_CLASS);

        // invoke the methods, 
        Object obj = a.newInstance();
        Method setURL = a.getMethod(SETCONFIGURL_METHOD, String.class);
        setURL.invoke(obj, configUrl);
        Method setConfig = a.getMethod(SETDATASOURCECONFIG_METHOD, Map.class);
        setConfig.invoke(obj, configMap);
        Method init = a.getMethod(INIT_METHOD, (Class[]) null);
        init.invoke(obj, (Object[]) null);

        return (DataSource) obj;
    }

    /**
     * 
     * 
     * @param jarPath
     * @param jarVersion
     * @param md5
     * @param configUrl
     * @param map
     * @return
     * @throws MalformedURLException 
     * @throws RemoteConfigurationException 
     * @throws InvocationTargetException 
     * @throws NoSuchMethodException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws ClassNotFoundException 
     * @throws IllegalArgumentException 
     * @throws SecurityException 
     * @throws Exception
     */
    public static DataSource loadCoreJar(String jarPath, String jarVersion, String md5,
                                         String configUrl, Map<String, String> map)
                                                                                   throws MalformedURLException,
                                                                                   RemoteConfigurationException,
                                                                                   SecurityException,
                                                                                   IllegalArgumentException,
                                                                                   ClassNotFoundException,
                                                                                   InstantiationException,
                                                                                   IllegalAccessException,
                                                                                   NoSuchMethodException,
                                                                                   InvocationTargetException {
        URL coreJarUrl = null;

        File jar = new File(FileUtil.LOCAL_JAR_PATH + jarVersion);
        if (jar.exists()) {
            if (logger.isInfoEnabled()) {
                logger.info("load local cache jar");
            }

            coreJarUrl = jar.toURI().toURL();
        } else {
            coreJarUrl = new URL(jarPath + jarVersion);
            FileUtil.retryUpdateFile(FileUtil.LOCAL_JAR_PATH, jarVersion, jarPath);
        }

        try {
            MD5Calculator.checkSignature(coreJarUrl, md5);
        } catch (ParameterException e) {// update core jar.
            FileUtil.retryUpdateFile(FileUtil.LOCAL_JAR_PATH, jarVersion, jarPath);
        } catch (NoSuchAlgorithmException e) {
            logger.error("", e);
        } catch (IOException e) {
            logger.error("", e);
        }

        return load(coreJarUrl, configUrl, map);
    }

}