package com.mn.cdc.exception;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-13 10:43
 **/
public class DatabaseHistoryException extends RuntimeException{
    private static final long serialVersionUID = 1L;

    public DatabaseHistoryException(String message) {
        super(message);
    }

    public DatabaseHistoryException(Throwable cause) {
        super(cause);
    }

    public DatabaseHistoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatabaseHistoryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
