package io.github.kafkaprinciple.common.errors;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public class NotFoundException extends ConnectException {
    public NotFoundException(String s) {
        super(s);
    }

    public NotFoundException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public NotFoundException(Throwable throwable) {
        super(throwable);
    }
}
