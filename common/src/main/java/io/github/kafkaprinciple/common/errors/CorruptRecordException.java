package io.github.kafkaprinciple.common.errors;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public class CorruptRecordException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public CorruptRecordException() {
        super("This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.");
    }

    public CorruptRecordException(String message) {
        super(message);
    }

    public CorruptRecordException(Throwable cause) {
        super(cause);
    }

    public CorruptRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
