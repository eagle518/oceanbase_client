package com.alipay.oceanbase.exception;

/**
 * InvalidException
 *
 * @author xiusiyan
 * @version 1.0, Apr 2, 2013
 * @see
 */
public class InvalidException extends DataSourceException {

    /**
     * 
     */
    public InvalidException() {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public InvalidException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public InvalidException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public InvalidException(Throwable cause) {
        super(cause);
    }

    private static final long serialVersionUID = 5854730814375688457L;

}
