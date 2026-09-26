package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.network.Send;
import io.github.kafkaprinciple.common.network.TransferableChannel;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class RecordsSend<T extends BaseRecords> implements Send {
    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

    private final T records;
    private final int maxBytesToWrite;
    private int remaining;
    private boolean pending = false;

    protected RecordsSend(T records, int maxBytesToWrite) {
        this.records = records;
        this.maxBytesToWrite = maxBytesToWrite;
        this.remaining = maxBytesToWrite;
    }

    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public final long writeTo(TransferableChannel channel) throws IOException {
        int written = 0;

        if (remaining > 0) {
            written = writeTo(channel, maxBytesToWrite - remaining, remaining);
            if (written < 0)
                throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
            remaining -= written;
        }

        pending = channel.hasPendingWrites();
        if (remaining <= 0 && pending)
            channel.write(EMPTY_BYTE_BUFFER);

        return written;
    }

    @Override
    public long size() {
        return maxBytesToWrite;
    }

    protected T records() {
        return records;
    }

    protected abstract int writeTo(TransferableChannel channel, int previouslyWritten, int remaining)
            throws IOException;
}
