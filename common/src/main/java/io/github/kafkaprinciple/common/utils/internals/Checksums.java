package io.github.kafkaprinciple.common.utils.internals;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

public final class Checksums {
    private Checksums() {
    }

    public static void update(Checksum checksum, ByteBuffer buffer, int length) {
        update(checksum, buffer, 0, length);
    }

    public static void update(Checksum checksum, ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray()) {
            checksum.update(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, length);
        } else if (buffer.isDirect()) {
            final int oldPosition = buffer.position();
            final int oldLimit = buffer.limit();

            try {
                final int start = oldPosition + offset;
                buffer.limit(start + length);
                checksum.update(buffer);
            } finally {
                buffer.limit(oldLimit);
                buffer.position(oldPosition);
            }
        } else {
            int start = buffer.position() + offset;
            for (int i = start; i < start + length; i++) {
                checksum.update(buffer.get(i));
            }
        }
    }

    public static void updateInt(Checksum checksum, int input) {
        checksum.update((byte) (input >> 24));
        checksum.update((byte) (input >> 16));
        checksum.update((byte) (input >> 8));
        checksum.update((byte) input);
    }

    public static void updateLong(Checksum checksum, long input) {
        checksum.update((byte) (input >> 56));
        checksum.update((byte) (input >> 48));
        checksum.update((byte) (input >> 40));
        checksum.update((byte) (input >> 32));
        checksum.update((byte) (input >> 24));
        checksum.update((byte) (input >> 16));
        checksum.update((byte) (input >> 8));
        checksum.update((byte) input);
    }
}
