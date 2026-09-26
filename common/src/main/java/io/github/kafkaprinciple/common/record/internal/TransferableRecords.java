package io.github.kafkaprinciple.common.record.internal;

import java.io.IOException;

import io.github.kafkaprinciple.common.network.TransferableChannel;

public interface TransferableRecords {
    int writeTo(TransferableChannel channel, int position, int length) throws IOException;
}
