package io.github.kafkaprinciple.common.record.internal;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalLong;

import io.github.kafkaprinciple.common.utils.internals.BufferSupplier;

public interface RecordBatch extends Iterable<Record>{
    byte MAGIC_VALUE_V0 = 0;
    byte MAGIC_VALUE_V1 = 1;
    byte MAGIC_VALUE_V2 = 2;
    byte CURRENT_MAGIC_VALUE = MAGIC_VALUE_V2;
    long NO_TIMESTAMP = -1L;
    long NO_PRODUCER_ID = -1L;
    short NO_PRODUCER_EPOCH = -1;
    int NO_SEQUENCE = -1;
    int NO_PARTITION_LEADER_EPOCH = -1;

    boolean isValid();

    void ensureValid();

    long checksum();

    long maxTimestamp();

    TimestampType TimestampType();

    long baseOffset();

    long lastOffset();

    long nextOffset();

    byte magic();

    long producerId();

    short producerEpoch();

    boolean hasProducerId();

    int baseSequence();

    int lastSequence();

    CompressionType compressionType();

    int sizeInBytes();

    Integer countOrNull();

    boolean isCompressed();

    void writeTo(ByteBuffer buffer);

    boolean isTransactional();

    OptionalLong deleteHorizonMs();

    int partitionLeaderEpoch();

    CloseableIterator<Record> streamingIterator(BufferSupplier decompressionBufferSupplier);

    CloseableIterator<Record> streamingIterator(BufferSupplier decompressionBufferSupplier, int maxRecordBodySize);

    boolean isControlBatch();

    default Optional<Long> offsetOfMaxTimestamp(int maxRecordBodySize) {
        if (magic() == RecordBatch.MAGIC_VALUE_V0)
            return Optional.empty();
        try (CloseableIterator<Record> iter = streamingIterator(BufferSupplier.create(), maxRecordBodySize)) {
            while (iter.hasNext()) {
                Record record = iter.next();
                if (maxTimestamp == record.timestamp())
                    return Optional.of(record.offset());
            }
        }
        return Optional.empty();
    }

}
