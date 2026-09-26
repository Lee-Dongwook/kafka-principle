package io.github.kafkaprinciple.common.config;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.errors.KafkaException;

@InterfaceAudience.Public
public class ConfigException extends KafkaException {
    private static final long serialVersionUID = 1L;

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String name, Object value) {
        this(name, value, null);
    }

    public ConfigException(String name, Object value, String message) {
        super("Invalid value " + value + " for configuration " + name + (message == null ? "" : ": " + message));
    }
}
