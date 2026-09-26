package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.errors.CorruptRecordException;

import java.nio.ByteBuffer;

import static io.github.kafkaprinciple.common.record.internal.Records.HEADER_SIZE_UP_TO_MAGIC;
import static io.github.kafkaprinciple.common.record.internal.Records.LOG_OVERHEAD;
import static io.github.kafkaprinciple.common.record.internal.Records.MAGIC_OFFSET;
import static io.github.kafkaprinciple.common.record.internal.Records.SIZE_OFFSET;

class ByteBufferLogInputStream implements LogInputStream<MutableRecordBatch> {
    private final ByteBuffer buffer;
    private final int maxMessageSize;

    ByteBufferLogInputStream(ByteBuffer buffer, int maxMessageSize) {
        this.buffer = buffer;
        this.maxMessageSize = maxMessageSize;
    }

    public MutableRecordBatch nextBatch() {
        int remaining = buffer.remaining();

        Integer batchSize = nextBatchSize();

        if (batchSize == null || remaining < batchSize) {
            return null;
        }

        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);

        ByteBuffer batchSlice = buffer.slice();
        batchSlice.limit(batchSize);
        buffer.position(buffer.position() + batchSize);

        if (magic > RecordBatch.MAGIC_VALUE_V1)
            return new DefaultRecordBatch(batchSlice);
        else
            return new AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch(batchSlice);
    }

    Integer nextBatchSize() throws CorruptRecordException {
        int remaining = buffer.remaining();
        if (remaining < LOG_OVERHEAD)
            return null;

        int recordSize = buffer.getInt(buffer.position() + SIZE_OFFSET);

        if (recordSize < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(
                    String.format("Record size %d is less than the minimum record overhead (%d)",
                            recordSize, LegacyRecord.RECORD_OVERHEAD_V0));

        if (recordSize > maxMessageSize)
            throw new CorruptRecordException(
                    String.format("Record size %d exceeds the largest allowable message size (%d).",
                            recordSize, maxMessageSize));

        if (remaining < HEADER_SIZE_UP_TO_MAGIC)
            return null;

        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);
        if (magic < 0 || magic > RecordBatch.CURRENT_MAGIC_VALUE)
            throw new CorruptRecordException("Invalid magic found in record: " + magic);

        return recordSize + LOG_OVERHEAD;
    }
}
