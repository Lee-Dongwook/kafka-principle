package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.network.TransferableChannel;
import io.github.kafkaprinciple.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class UnalignedMemoryRecords implements UnalignedRecords {
    private static final UnalignedMemoryRecords EMPTY = new UnalignedMemoryRecords(ByteBuffer.allocate(0));

    private final ByteBuffer buffer;

    public UnalignedMemoryRecords(ByteBuffer buffer) {
        this.buffer = Objects.requireNonNull(buffer);
    }

    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    @Override
    public int sizeInBytes() {
        return buffer.remaining();
    }

    @Override
    public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
        if (((long) position) + length > buffer.limit())
            throw new IllegalArgumentException("position+length should not be greater than buffer.limit(), position: "
                    + position + ", length: " + length + ", buffer.limit(): " + buffer.limit());
        return Utils.tryWriteTo(channel, position, length, buffer);
    }

    public static UnalignedMemoryRecords empty() {
        return EMPTY;
    }
}
