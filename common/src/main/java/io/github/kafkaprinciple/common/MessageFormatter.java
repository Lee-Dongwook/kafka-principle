package io.github.kafkaprinciple.common;

import io.github.kafkaprinciple.clients.consumer.ConsumerRecord;
import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.io.Closeable;
import java.io.PrintStream;
import java.util.Map;

@InterfaceAudience.Public
public interface MessageFormatter extends Configurable, Closeable {
    default void configure(Map<String, ?> configs) {
    }

    void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output);

    default void close() {
    }
}
