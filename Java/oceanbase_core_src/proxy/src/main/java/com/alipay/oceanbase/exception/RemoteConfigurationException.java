package com.alipay.oceanbase.exception;

/**
 * 
 * 
 * @author liangjieli
 * @version $Id: RemoteConfigurationException.java, v 0.1 Jun 6, 2013 11:56:57 AM liangjieli Exp $
 */
public class RemoteConfigurationException extends Exception {

    /**  */
    private static final long serialVersionUID = 4628988874782122394L;

    public RemoteConfigurationException(String configuration) {
        super(String.format("please contact DBA, %s not config", configuration));
    }

    public RemoteConfigurationException(String message, Exception exception) {
        super(message, exception);
    }
}