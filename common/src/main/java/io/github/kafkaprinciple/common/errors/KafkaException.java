package io.github.kafkaprinciple.common.errors;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public class KafkaException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public KafkaException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(Throwable cause) {
        super(cause);
    }

    public KafkaException() {
        super();
    }
}
