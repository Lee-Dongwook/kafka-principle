package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.network.TransferableChannel;

import java.io.IOException;

public class DefaultRecordsSend<T extends TransferableChannel> extends RecordsSend<T> {
    public DefaultRecordsSend(T records) {
        this(records, records.sizeInBytes());
    }

    public DefaultRecordsSend(T records, int maxBytesToWrite) {
        super(records, maxBytesToWrite);
    }

    @Override
    protected int writeTo(TransferableChannel channel, int previouslyWritten, int remaining) throws IOException {
        return records().writeTo(channel, previouslyWritten, remaining);
    }
}
