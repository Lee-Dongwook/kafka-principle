package io.github.kafkaprinciple.common.errors;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.annotation.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BootstrapResolutionException extends KafkaException {
    public BootstrapResolutionException(String message) {
        super(message);
    }

    public BootstrapResolutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
