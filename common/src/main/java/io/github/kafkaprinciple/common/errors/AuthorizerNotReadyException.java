package io.github.kafkaprinciple.common.errors;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public class AuthorizerNotReadyException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public AuthorizerNotReadyException() {
        super();
    }
}
