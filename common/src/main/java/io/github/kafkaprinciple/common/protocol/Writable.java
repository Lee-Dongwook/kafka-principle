package io.github.kafkaprinciple.common.protocol;

import io.github.kafkaprinciple.common.Uuid;
import io.github.kafkaprinciple.common.record.internal.BaseRecords;
import io.github.kafkaprinciple.common.record.internal.MemoryRecords;
import io.github.kafkaprinciple.common.record.internal.UnalignedMemoryRecords;

import java.nio.ByteBuffer;

public interface Writable {
    void writeByte(byte val);

    void writeShort(short val);

    void writeInt(int val);

    void writeLong(long val);

    void writeDouble(double val);

    void writeByteArray(byte[] arr);

    void writeUnsignedVarint(int i);

    void writeByteBuffer(ByteBuffer buf);

    void writeVarint(int i);

    void writeVarlong(long i);

    default void writeRecords(BaseRecords records) {
        if (records instanceof MemoryRecords) {
            MemoryRecords memRecords = (MemoryRecords) records;
            writeByteBuffer(memRecords.buffer());
        } else if (records instanceof UnalignedMemoryRecords) {
            UnalignedMemoryRecords memRecords = (UnalignedMemoryRecords) records;
            writeByteBuffer(memRecords.buffer());
        } else {
            throw new UnsupportedOperationException("Unsupported record type " + records.getClass());
        }
    }

    default void writeUuid(Uuid uuid) {
        writeLong(uuid.getMostSignificantBits());
        writeLong(uuid.getLeastSignificantBits());
    }

    default void writeUnsignedShort(int i) {
        writeShort((short) i);
    }

    default void writeUnsignedInt(long i) {
        writeInt((int) i);
    }
}
