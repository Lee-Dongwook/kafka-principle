package io.github.kafkaprinciple.server.util;

import io.github.kafkaprinciple.common.errors.KafkaException;

public class TerseFailure extends KafkaException {
    public TerseFailure(String message) {
        super(message);
    }

    public TerseFailure(String message, Throwable cause) {
        super(message, cause);
    }
}
