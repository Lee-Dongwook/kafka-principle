package io.github.kafkaprinciple.common.serialization;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@FunctionalInterface
@InterfaceAudience.Public
public interface Decoder<T> {
    T fromBytes(byte[] bytes);
}
