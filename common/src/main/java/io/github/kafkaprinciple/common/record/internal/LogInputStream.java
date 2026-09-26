package io.github.kafkaprinciple.common.record.internal;

import java.io.IOException;

interface LogInputStream<T extends RecordBatch> {
    T nextBatch() throws IOException;
}