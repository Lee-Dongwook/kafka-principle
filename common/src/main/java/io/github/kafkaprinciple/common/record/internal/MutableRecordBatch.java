package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.record.TimestampType;
import io.github.kafkaprinciple.common.utils.internals.BufferSupplier;
import io.github.kafkaprinciple.common.utils.internals.CloseableIterator;

public interface MutableRecordBatch extends RecordBatch {
    void setLastOffset(long offset);

    void setMaxTimestamp(TimestampType timestampType, long maxTimestamp);

    void setPartiotionLeaderEpoch(int epoch);

    void writeTo(ByteBufferOutputStream outputStream);

    CloseableIterator<Record> skipKeyValueIterator(BufferSupplier bufferSupplier);

    CloseableIterator<Record> skipKeyValueIterator(BufferSupplier bufferSupplier, int maxRecordBodySize);
}
