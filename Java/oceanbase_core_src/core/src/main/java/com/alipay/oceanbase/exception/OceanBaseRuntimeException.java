package com.alipay.oceanbase.exception;

/**
 * OceanBaseRuntimeException
 *
 * @author xiusiyan
 * @version 1.0, Apr 2, 2013
 * @see
 */
public class OceanBaseRuntimeException extends RuntimeException {

    /**
     * 
     */
    public OceanBaseRuntimeException() {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public OceanBaseRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public OceanBaseRuntimeException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public OceanBaseRuntimeException(Throwable cause) {
        super(cause);
    }

    private static final long serialVersionUID = -7782470037655652218L;

}
