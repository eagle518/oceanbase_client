package com.alipay.oceanbase.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.log4j.Logger;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: FileUtil.java, v 0.1 2013-5-24 下午1:29:31 liangjie.li Exp $
 */
public class FileUtil {

    private static final Logger logger         = Logger.getLogger(FileUtil.class);

    public static final int     RETRY_TIMES    = 3;
    public static final String  LOCAL_JAR_PATH = System.getProperty("user.home")
                                                 + "/.obdatasource/";

    /**
     * 
     * 
     * @param file
     * @param url
     */
    public static void updateFile(String filePath, String fileName, URL url) {
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
        } catch (Exception ex) {
            logger.warn("write local cache fail, path:" + filePath + "/" + fileName + " due to "
                        + ex.getMessage());
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
