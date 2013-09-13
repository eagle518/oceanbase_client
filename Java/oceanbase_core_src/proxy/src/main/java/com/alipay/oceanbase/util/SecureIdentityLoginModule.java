package com.alipay.oceanbase.util;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

/**
 * 
 * 
 * @author liangjieli
 * @version $Id: SecureIdentityLoginModule.java, v 0.1 Aug 8, 2012 11:18:04 AM liangjieli Exp $
 */
public class SecureIdentityLoginModule {

    private static byte[] ENC_KEY_BYTES_PROD = null;

    /**
     * 
     */
    static {
        try {
            ENC_KEY_BYTES_PROD = "gQzLk5tTcGYlQ47GG29xQxfbHIURCheJ".getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    /**
     * 
     * 
     * @param secret
     * @return
     * @throws NoSuchPaddingException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws BadPaddingException
     * @throws IllegalBlockSizeException
     * @throws UnsupportedEncodingException
     */
    public static String encode(String secret) throws NoSuchPaddingException,
                                              NoSuchAlgorithmException, InvalidKeyException,
                                              BadPaddingException, IllegalBlockSizeException,
                                              UnsupportedEncodingException {
        return SecureIdentityLoginModule.encode(null, secret);
    }

    /**
     * 
     * 
     * @param encKey
     * @param secret
     * @return
     * @throws InvalidKeyException
     * @throws NoSuchAlgorithmException
     * @throws NoSuchPaddingException
     * @throws IllegalBlockSizeException
     * @throws BadPaddingException
     * @throws UnsupportedEncodingException
     */
    public static String encode(String encKey, String secret) throws InvalidKeyException,
                                                             NoSuchAlgorithmException,
                                                             NoSuchPaddingException,
                                                             IllegalBlockSizeException,
                                                             BadPaddingException,
                                                             UnsupportedEncodingException {
        byte[] kbytes = SecureIdentityLoginModule.ENC_KEY_BYTES_PROD;
        if (isNotBlank(encKey)) {
            kbytes = encKey.getBytes("UTF-8");
        }

        return initEncode(kbytes, secret);
    }

    /**
     * 
     * 
     * @param kbytes
     * @param secret
     * @return
     * @throws NoSuchAlgorithmException
     * @throws NoSuchPaddingException
     * @throws InvalidKeyException
     * @throws IllegalBlockSizeException
     * @throws BadPaddingException
     * @throws UnsupportedEncodingException
     */
    static final String initEncode(byte[] kbytes, String secret) throws NoSuchAlgorithmException,
                                                                NoSuchPaddingException,
                                                                InvalidKeyException,
                                                                IllegalBlockSizeException,
                                                                BadPaddingException,
                                                                UnsupportedEncodingException {
        SecretKeySpec key = new SecretKeySpec(kbytes, "Blowfish");
        Cipher cipher = Cipher.getInstance("Blowfish");
        cipher.init(Cipher.ENCRYPT_MODE, key);
        byte[] encoding = cipher.doFinal(secret.getBytes("UTF-8"));
        BigInteger n = new BigInteger(encoding);
        return n.toString(16);
    }

    /**
     * 
     * 
     * @param secret
     * @return
     * @throws NoSuchPaddingException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws BadPaddingException
     * @throws IllegalBlockSizeException
     * @throws UnsupportedEncodingException
     */
    public static String decode(String secret) throws NoSuchPaddingException,
                                              NoSuchAlgorithmException, InvalidKeyException,
                                              BadPaddingException, IllegalBlockSizeException,
                                              UnsupportedEncodingException {
        return SecureIdentityLoginModule.decode(null, secret);
    }

    /**
     * 
     * 
     * @param encKey
     * @param secret
     * @return
     * @throws NoSuchPaddingException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws BadPaddingException
     * @throws IllegalBlockSizeException
     * @throws UnsupportedEncodingException
     */
    public static String decode(String encKey, String secret) throws NoSuchPaddingException,
                                                             NoSuchAlgorithmException,
                                                             InvalidKeyException,
                                                             BadPaddingException,
                                                             IllegalBlockSizeException,
                                                             UnsupportedEncodingException {

        byte[] kbytes = SecureIdentityLoginModule.ENC_KEY_BYTES_PROD;
        if (isNotBlank(encKey)) {
            kbytes = encKey.getBytes("utf-8");
        }

        return iniDecode(kbytes, secret);
    }

    /**
     * 
     * 
     * @param kbytes
     * @param secret
     * @return
     * @throws NoSuchPaddingException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws BadPaddingException
     * @throws IllegalBlockSizeException
     */
    static final String iniDecode(byte[] kbytes, String secret) throws NoSuchPaddingException,
                                                               NoSuchAlgorithmException,
                                                               InvalidKeyException,
                                                               BadPaddingException,
                                                               IllegalBlockSizeException {
        SecretKeySpec key = new SecretKeySpec(kbytes, "Blowfish");
        BigInteger n = new BigInteger(secret, 16);
        byte[] encoding = n.toByteArray();
        // SECURITY-344: fix leading zeros
        if (encoding.length % 8 != 0) {
            int length = encoding.length;
            int newLength = ((length / 8) + 1) * 8;
            int pad = newLength - length; //number of leading zeros
            byte[] old = encoding;
            encoding = new byte[newLength];
            for (int i = old.length - 1; i >= 0; i--) {
                encoding[i + pad] = old[i];
            }
        }
        Cipher cipher = Cipher.getInstance("Blowfish");
        cipher.init(Cipher.DECRYPT_MODE, key);
        byte[] decode = cipher.doFinal(encoding);
        return new String(decode);
    }

    /**
     * 
     * 
     * @param str
     * @return
     */
    static final boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    /**
     * 
     * 
     * @param str
     * @return
     */
    static final boolean isBlank(String str) {
        int strLen = 0;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((Character.isWhitespace(str.charAt(i)) == false)) {
                return false;
            }
        }
        return true;
    }

}
