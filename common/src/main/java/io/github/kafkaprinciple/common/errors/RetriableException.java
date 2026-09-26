package io.github.kafkaprinciple.common.errors;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public abstract class RetriableException extends ApiException {
    private static final long serialVersionUID = 1L;

    public RetriableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetriableException(String message) {
        super(message);
    }

    public RetriableException(Throwable cause) {
        super(cause);
    }

    public RetriableException() {
    }
}
