package com.alipay.oceanbase;

import static com.alipay.oceanbase.log.CommonLoggerComponent.PORXY_MODULE_LOGGER_NAME;

import java.io.Serializable;
import java.net.URL;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.alipay.oceanbase.exception.RemoteConfigurationException;
import com.alipay.oceanbase.util.ConfigLoader;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: RemoteConfigs.java, v 0.1 2013-5-24 下午1:43:22 liangjie.li Exp $
 */
public class RemoteConfigs implements Serializable {

    /**  */
    private static final long   serialVersionUID = 377204585880051165L;

    private static final Logger logger           = Logger.getLogger(PORXY_MODULE_LOGGER_NAME);

    static final String         USER_NAME        = "username";
    static final String         PASSWORD         = "password";
    static final String         CLUSTER_ADDRESS  = "clusterAddress";
    static final String         DEFAULT_VALUE    = "";

    private String              clusterAddress;
    private String              coreJarPath;
    private String              coreJarVersion;
    private String              MD5;
    private String              whiteList;
    private String              userName;
    private String              password;
    private int                 percentage;
    private boolean             enableUpdate;

    /**
     * 
     * @param clusterAddress
     * @param coreJarPath
     * @param MD5
     * @param whiteList
     * @param userName
     * @param password
     */
    private RemoteConfigs(String clusterAddress, String coreJarPath, String coreJarVersion,
                          String MD5, String whiteList, String userName, String password,
                          int percentage, boolean enableUpdate) {
        this.clusterAddress = clusterAddress;
        this.coreJarPath = coreJarPath;
        this.coreJarVersion = coreJarVersion;
        this.MD5 = MD5;
        this.whiteList = whiteList;
        this.userName = userName;
        this.password = password;
        this.percentage = percentage;
        this.enableUpdate = enableUpdate;
    }

    /**
     * 
     * 
     * @param properties
     * @return
     * @throws RemoteConfigurationException 
     */
    public static RemoteConfigs getInstance(URL configUrl) throws RemoteConfigurationException {
        if (logger.isDebugEnabled()) {
            logger.debug("oceanbase remote configuration has refesh");
        }

        return RemoteConfigs.newInstance(ConfigLoader.load(configUrl));
    }

    /**
     * 
     * 
     * @param properties
     * @return
     */
    public static RemoteConfigs newInstance(Properties properties) {
        String clusterAddress = properties.getProperty("clusterAddress", DEFAULT_VALUE);
        String coreJarPath = properties.getProperty("coreJarPath", DEFAULT_VALUE);
        String coreJarVersion = properties.getProperty("coreJarVersion", DEFAULT_VALUE);
        String MD5 = properties.getProperty("MD5", DEFAULT_VALUE);
        String whiteList = properties.getProperty("whiteList", DEFAULT_VALUE);
        String userName = properties.getProperty("username", DEFAULT_VALUE);
        String password = properties.getProperty("password", DEFAULT_VALUE);

        int percentage = 0;
        try {
            percentage = Integer.parseInt(properties.getProperty("percentage", DEFAULT_VALUE));
        } catch (NumberFormatException e) {
            // ignore
        }
        boolean enableUpdate = Boolean.parseBoolean(properties.getProperty("enableUpdate",
            DEFAULT_VALUE));

        return new RemoteConfigs(clusterAddress, coreJarPath, coreJarVersion, MD5, whiteList,
            userName, password, percentage, enableUpdate);
    }

    // //////////////////////////////// setter and getter ////////////////////////////////
    public String getClusterAddress() {
        return clusterAddress;
    }

    public void setClusterAddress(String clusterAddress) {
        this.clusterAddress = clusterAddress;
    }

    public String getCoreJarPath() {
        return coreJarPath;
    }

    public void setCoreJarPath(String coreJarPath) {
        this.coreJarPath = coreJarPath;
    }

    public String getCoreJarVersion() {
        return coreJarVersion;
    }

    public void setCoreJarVersion(String coreJarVersion) {
        this.coreJarVersion = coreJarVersion;
    }

    public String getMD5() {
        return MD5;
    }

    public void setMD5(String mD5) {
        MD5 = mD5;
    }

    public String getWhiteList() {
        return whiteList;
    }

    public void setWhiteList(String whiteList) {
        this.whiteList = whiteList;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPercentage() {
        return percentage;
    }

    public void setPercentage(int percentage) {
        this.percentage = percentage;
    }

    public boolean isEnableUpdate() {
        return enableUpdate;
    }

    public void setEnableUpdate(boolean enableUpdate) {
        this.enableUpdate = enableUpdate;
    }

}
