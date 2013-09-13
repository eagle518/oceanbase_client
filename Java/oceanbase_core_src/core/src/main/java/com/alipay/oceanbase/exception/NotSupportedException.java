package com.alipay.oceanbase.exception;

/**
 * NotSupportException
 *
 * @author xiusiyan
 * @version 1.0, Apr 3, 2013
 * @see
 */
public class NotSupportedException extends OceanBaseRuntimeException {

    /**
     * 
     */
    public NotSupportedException() {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public NotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public NotSupportedException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public NotSupportedException(Throwable cause) {
        super(cause);
    }

    private static final long serialVersionUID = 566108034415530606L;

}
