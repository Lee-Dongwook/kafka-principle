package io.github.kafkaprinciple.common.record.internal;

import java.io.EOFException;
import java.io.IOException;

import io.github.kafkaprinciple.common.errors.CorruptRecordException;
import io.github.kafkaprinciple.common.errors.KafkaException;
import io.github.kafkaprinciple.common.utils.internals.AbstractIterator;

class RecordBatchIterator<T extends RecordBatch> extends AbstractIterator<T> {
    private final LogInputStream<T> logInputStream;

    RecordBatchIterator(LogInputStream<T> logInputStream) {
        this.logInputStream = logInputStream;
    }

    @Override
    protected T makeNext() {
        try {
            T batch = logInputStream.nextBatch();
            if (batch == null)
                return allDone();
            return batch;
        } catch (EOFException e) {
            throw new CorruptRecordException("Unexpected EOF while attempting to read the next batch", e);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }
}
