package org.apache.phoenix.ddb.service.exceptions;

public class PhoenixServiceException extends RuntimeException {
    public PhoenixServiceException(String message) {
        super(message);
    }

    public PhoenixServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public PhoenixServiceException(Throwable cause) {
        super(cause);
    }
}
