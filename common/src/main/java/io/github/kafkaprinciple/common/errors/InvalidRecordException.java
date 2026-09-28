package io.github.kafkaprinciple.common.errors;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public class InvalidRecordException extends InvalidConfigurationException {
    private static final long serialVersionUID = 1;

    public InvalidRecordException(String s) {
        super(s);
    }

    public InvalidRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
