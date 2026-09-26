package io.github.kafkaprinciple.common.network;

import java.io.IOException;

public interface Send {
    boolean completed();

    long writeTo(TransferableChannel channel) throws IOException;

    long size();
}
