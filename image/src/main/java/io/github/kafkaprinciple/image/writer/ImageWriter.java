package io.github.kafkaprinciple.image.writer;

import io.github.kafkaprinciple.common.protocol.ApiMessage;
import io.github.kafkaprinciple.server.common.ApiMessageAndVersion;

public interface ImageWriter extends AutoCloseable {
    default void write(int version, ApiMessage message) {
        write(new ApiMessageAndVersion(message, (short) version));
    }

    void write(ApiMessageAndVersion record);

    default void close() {
        close(false);
    }

    void close(boolean complete);
}
