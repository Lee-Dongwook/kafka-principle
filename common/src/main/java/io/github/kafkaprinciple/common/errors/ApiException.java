package io.github.kafkaprinciple.common.errors;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public class ApiException extends KafkaException {
    private static final long serialVersionUID = 1L;

    public ApiException(String message, Throwable cause) {
        super(message, cause);
    }

    public ApiException(String message) {
        super(message);
    }

    public ApiException(Throwable cause) {
        super(cause);
    }

    public ApiException() {
        super();
    }
    
    @Override 
    public Throwable fillInStackTrace() {
        return this;
    }
}
