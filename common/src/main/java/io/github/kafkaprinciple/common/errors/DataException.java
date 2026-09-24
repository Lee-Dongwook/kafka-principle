package io.github.kafkaprinciple.common.errors;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public class DataException extends ConnectException {
    public DataException(String s) {
        super(s);
    }

    public DataException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public DataException(Throwable throwable) {
        super(throwable);
    }
}
