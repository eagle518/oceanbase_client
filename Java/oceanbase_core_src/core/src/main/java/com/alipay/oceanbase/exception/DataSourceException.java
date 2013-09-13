package com.alipay.oceanbase.exception;

/**
 * DataSourceException
 *
 * @author xiusiyan
 * @version 1.0, Apr 2, 2013
 * @see
 */
public class DataSourceException extends OceanBaseRuntimeException {

    /**
     * 
     */
    public DataSourceException() {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public DataSourceException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public DataSourceException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public DataSourceException(Throwable cause) {
        super(cause);
    }

    private static final long serialVersionUID = 2870323426913660404L;

}
