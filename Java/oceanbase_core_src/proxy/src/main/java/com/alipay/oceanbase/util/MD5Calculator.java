package com.alipay.oceanbase.util;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.alipay.oceanbase.exception.ParameterException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: MD5Calculator.java, v 0.1 2013-5-24 下午1:00:11 liangjie.li Exp $
 */
public class MD5Calculator {

    /**
     * 
     * 
     * @param is
     * @return
     * @throws NoSuchAlgorithmException
     * @throws IOException
     */
    public static String calcuateJarFromInputStream(InputStream is)
                                                                   throws NoSuchAlgorithmException,
                                                                   IOException {
        MessageDigest md = MessageDigest.getInstance("MD5");

        byte[] buffer = new byte[1024];
        int numRead = 0;
        do {
            numRead = is.read(buffer);
            if (numRead > 0) {
                md.update(buffer, 0, numRead);
            }
        } while (numRead != -1);

        byte[] digest = md.digest();
        BigInteger bigInt = new BigInteger(1, digest);
        return bigInt.toString(16);
    }

    /**
     * 
     * 
     * @param url
     * @param md5
     * @throws IOException  
     * @throws NoSuchAlgorithmException 
     * @throws ParameterException 
     */
    public static void checkSignature(URL url, String md5) throws NoSuchAlgorithmException,
                                                          IOException, ParameterException {
        InputStream stream = null;
        try {
            String result = MD5Calculator.calcuateJarFromInputStream(url.openStream());
            if (!md5.equals(result)) {
                throw new ParameterException(String.format(
                    "md5 not consistent, expect[%s], actual[%s]", md5, result));
            }
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    private MD5Calculator() {
    }
}