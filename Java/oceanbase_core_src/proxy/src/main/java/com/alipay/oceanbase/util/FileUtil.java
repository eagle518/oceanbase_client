package com.alipay.oceanbase.util;

import static com.alipay.oceanbase.log.CommonLoggerComponent.PORXY_MODULE_LOGGER_NAME;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.log4j.Logger;

import com.alipay.oceanbase.exception.RemoteConfigurationException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: FileUtil.java, v 0.1 2013-5-24 下午1:29:31 liangjie.li Exp $
 */
public class FileUtil {

    private static final Logger logger         = Logger.getLogger(PORXY_MODULE_LOGGER_NAME);

    public static final int     RETRY_TIMES    = 3;
    public static final String  LOCAL_JAR_PATH = System.getProperty("user.home")
                                                 + "/.obdatasource/";

    /**
     * 
     * 
     * @param filePath
     * @param fileName
     * @param jarVersion
     * @param jarPath
     * @throws RemoteConfigurationException
     */
    public static void retryUpdateFile(String filePath, String jarVersion, String jarPath)
                                                                                          throws RemoteConfigurationException {
        int i = 0;
        while (i < RETRY_TIMES) {
            try {
                FileUtil.updateFile(FileUtil.LOCAL_JAR_PATH, jarVersion, new URL(jarPath
                                                                                 + jarVersion));
                break;
            } catch (IOException e) {
                if (logger.isInfoEnabled() && i < RETRY_TIMES - 1) {
                    logger.info("will retry to save remote jar, the times: " + (i + 1));
                } else {
                    throw new RemoteConfigurationException(String.format(
                        "load remote jar fail[%s]", jarPath + jarVersion), e);
                }
                i++;
            }
        }
    }

    /**
     * 
     * 
     * @param file
     * @param url
     * @throws IOException 
     */
    public static void updateFile(String filePath, String fileName, URL url) throws IOException {
        File cacheFilePath = new File(filePath);
        FileOutputStream out = null;
        InputStream stream = null;
        try {
            if (!cacheFilePath.exists()) {
                cacheFilePath.mkdirs();
            }

            File cacheFile = new File(cacheFilePath, fileName);
            if (!cacheFile.exists()) {
                cacheFile.createNewFile();
            }

            out = new FileOutputStream(cacheFile);
            stream = url.openStream();
            byte[] content = new byte[1024];
            int length = 0;
            while ((length = stream.read(content)) != -1) {
                out.write(content, 0, length);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("write local cache success, file: " + filePath + fileName);
            }
        } catch (IOException e) {
            logger.warn("write local cache fail, path:" + filePath + "/" + fileName + " due to "
                        + e.getMessage());
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ex) {
                    logger.warn("file outstream close fail", ex);
                }
            }
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException ex) {
                    logger.warn("url stream close fail", ex);
                }
            }
        }
    }
}
