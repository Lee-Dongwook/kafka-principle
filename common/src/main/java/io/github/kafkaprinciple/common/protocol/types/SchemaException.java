package io.github.kafkaprinciple.common.protocol.types;

import io.github.kafkaprinciple.common.errors.KafkaException;

public class SchemaException extends KafkaException{
    private static final long serialVersionUID = 1L;
    
    public SchemaException(String message) {
        super(message);
    }

    public SchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
