
package io.github.kafkaprinciple.common.protocol;

import io.github.kafkaprinciple.common.Uuid;
import io.github.kafkaprinciple.common.protocol.types.RawTaggedField;
import io.github.kafkaprinciple.common.record.internal.MemoryRecords;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public interface Readable {
    byte readByte();
    short readShort();
    int readInt();
    long readLong();
    double readDouble();
    byte[] readArray(int length);
    int readUnsignedVarint();
    ByteBuffer readByteBuffer(int length);
    int readVarint();
    long readVarlong();
    int remaining();

        Readable slice();

    default String readString(int length) {
        byte[] arr = readArray(length);
        return new String(arr, StandardCharsets.UTF_8);
    }

    default List<RawTaggedField> readUnknownTaggedField(List<RawTaggedField> unknowns, int tag, int size) {
        if (unknowns == null) {
            unknowns = new ArrayList<>();
        }
        byte[] data = readArray(size);
        unknowns.add(new RawTaggedField(tag, data));
        return unknowns;
    }

    default MemoryRecords readRecords(int length) {
        if (length < 0) {
            return null;
        } else {
            ByteBuffer recordsBuffer = readByteBuffer(length);
            return MemoryRecords.readableRecords(recordsBuffer);
        }
    }

        default Uuid readUuid() {
        return new Uuid(readLong(), readLong());
    }

    default int readUnsignedShort() {
        return Short.toUnsignedInt(readShort());
    }

    default long readUnsignedInt() {
        return Integer.toUnsignedLong(readInt());
    }
}
