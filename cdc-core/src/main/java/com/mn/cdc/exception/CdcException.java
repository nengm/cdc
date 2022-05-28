package com.mn.cdc.exception;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-10-11 15:42
 **/
public class CdcException extends RuntimeException{

    private static final long serialVersionUID = 1675762975448541722L;

    public CdcException() {
    }

    public CdcException(String message) {
        super(message);
    }

    public CdcException(Throwable cause) {
        super(cause);
    }

    public CdcException(String message, Throwable cause) {
        super(message, cause);
    }

    public CdcException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
