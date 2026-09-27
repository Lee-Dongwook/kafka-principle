package io.github.kafkaprinciple.common;

import java.util.Map;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public interface Configurable {
    void configure(Map<String, ?> configs);
}
