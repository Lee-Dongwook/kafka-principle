package io.github.kafkaprinciple.common.utils.internals;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

public final class Crc32C {
    private Crc32C() {
    }

    public static long compute(byte[] bytes, int offset, int size) {
        Checksum crc = new CRC32C();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }

    public static long compute(ByteBuffer buffer, int offset, int size) {
        Checksum crc = new CRC32C();
        Checksums.update(crc, buffer, offset, size);
        return crc.getValue();
    }
}
