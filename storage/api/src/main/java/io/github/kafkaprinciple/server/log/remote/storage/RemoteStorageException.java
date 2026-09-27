package io.github.kafkaprinciple.server.log.remote.storage;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

@InterfaceAudience.Public
public class RemoteStorageException extends Exception {
    private static final long serialVersionUID = 1L;

    public RemoteStorageException(final String message) {
        super(message);
    }

    public RemoteStorageException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public RemoteStorageException(Throwable cause) {
        super(cause);
    }
}
