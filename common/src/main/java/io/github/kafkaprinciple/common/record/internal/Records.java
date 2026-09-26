package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.utils.internals.AbstractIterator;

import java.util.Iterator;
import java.util.Optional;

public interface Records extends TransferableRecords {
    int OFFSET_OFFSET = 0;
    int OFFSET_LENGTH = 8;
    int SIZE_OFFSET = OFFSET_OFFSET + OFFSET_LENGTH;
    int SIZE_LENGTH = 4;
    int LOG_OVERHEAD = SIZE_OFFSET + SIZE_LENGTH;
    int MAGIC_OFFSET = LOG_OVERHEAD + 4;
    int MAGIC_LENGTH = 1;
    int HEADER_SIZE_UP_TO_MAGIC = MAGIC_OFFSET + MAGIC_LENGTH;
    int SOFT_MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;

    Iterable<? extends RecordBatch> batches();

    AbstractIterator<? extends RecordBatch> batchIterator();

    Optional<RecordBatch> lastBatch();

    boolean hasMatchingMagic(byte magic);

    Iterable<Record> records();

    Records slice(int position, int size);
}
