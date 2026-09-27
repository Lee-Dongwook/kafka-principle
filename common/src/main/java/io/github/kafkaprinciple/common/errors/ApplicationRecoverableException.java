package io.github.kafkaprinciple.common.errors;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public abstract class ApplicationRecoverableException extends ApiException {
    private static final long serialVersionUID = 1L;

    public ApplicationRecoverableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ApplicationRecoverableException(String message) {
        super(message);
    }

    public ApplicationRecoverableException(Throwable cause) {
        super(cause);
    }

    public ApplicationRecoverableException() {
        super();
    }
}
