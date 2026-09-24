package io.github.kafkaprinciple.connect.api.health;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Locale;

@InterfaceAudience.Public
public enum ConnectorType {
    SOURCE,
    SINK,
    UNKNOWN;
    
    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }
}
