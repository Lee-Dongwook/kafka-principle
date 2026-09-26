package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.header.Header;
import io.github.kafkaprinciple.common.utils.Utils;
import io.github.kafkaprinciple.common.utils.internals.AbstractIterator;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;

public abstract class AbstractRecords implements Records {
    private final Iterable<Record> records = this::recordsIterator;

    @Override 
    public boolean hasMatchingMagic(byte magic) {
        for (RecordBatch batch : batches())
            if (batch.magic() != magic)
                return false;
        return true;
    }

    public RecordBatch firstBatch() {
        Iterator<? extends RecordBatch> iterator = batches().iterator();

        if (!iterator.hasNext())
            return null;

        return iterator.next();
    }
    
    @Override
    public Optional<RecordBatch> lastBatch() {
        Iterator<? extends RecordBatch> iterator = batches().iterator();

        RecordBatch batch = null;
        while (iterator.hasNext())
            batch = iterator.next();

        return Optional.ofNullable(batch);
    }

    @Override
    public Iterable<Record> records() {
        return records;
    }

    @Override
    public DefaultRecordsSend<Records> toSend() {
        return new DefaultRecordsSend<>(this);
    }

    private Iterator<Record> recordsIterator() {
        return new AbstractIterator<>() {
            private final Iterator<? extends RecordBatch> batches = batches().iterator();
            private Iterator<Record> records;

            @Override
            protected Record makeNext() {
                if (records != null && records.hasNext())
                    return records.next();

                if (batches.hasNext()) {
                    records = batches.next().iterator();
                    return makeNext();
                }
                return allDone();
            }
        };
    }
    
    public static int estimateSizeInBytes(byte magic,
                long baseOffset,
                CompressionType compressionType,
                Iterable<Record> records
    ) {
        int size = 0;
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (Record record : records) {
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
            }
        } else {
            size = DefaultRecordBatch.sizeInBytes(baseOffset, records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    public static int estimateSizeInBytes(byte magic,
                CompressionType compressionType,
                Iterable<SimpleRecord> records
    ) {
        int size = 0;
        if (magic <= RecordBatch.MAGIC_VALUE_V1) {
            for (SimpleRecord record : records)
                size += Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, record.key(), record.value());
        } else {
            size = DefaultRecordBatch.sizeInBytes(records);
        }
        return estimateCompressedSizeInBytes(size, compressionType);
    }

    private static int estimateCompressedSizeInBytes(int size, CompressionType compressionType) {
        return compressionType == CompressionType.NONE ? size : Math.min(Math.max(size / 2, 1024), 1 << 16);
    }

    public static int estimateSizeInBytesUpperBound(byte magic, CompressionType compressionType, byte[] key, byte[] value, Header[] headers) {
        return estimateSizeInBytesUpperBound(magic, compressionType, Utils.wrapNullable(key), Utils.wrapNullable(value), headers);
    }

    public static int estimateSizeInBytesUpperBound(byte magic, CompressionType compressionType, ByteBuffer key,
            ByteBuffer value, Header[] headers) {
        if (magic >= RecordBatch.MAGIC_VALUE_V2)
            return DefaultRecordBatch.estimateBatchSizeUpperBound(key, value, headers);
        else if (compressionType != CompressionType.NONE)
            return Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic)
                    + LegacyRecord.recordSize(magic, key, value);
        else
            return Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
    }
    
    public static int recordBatchHeaderSizeInBytes(byte magic, CompressionType compressionType) {
        if (magic > RecordBatch.MAGIC_VALUE_V1) {
            return DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        } else if (compressionType != CompressionType.NONE) {
            return Records.LOG_OVERHEAD + LegacyRecord.recordOverhead(magic);
        } else {
            return 0;
        }
    }
}
