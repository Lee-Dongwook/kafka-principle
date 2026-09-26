package io.github.kafkaprinciple.common.network;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;

public interface TransferableChannel extends GatheringByteChannel {
    boolean hasPendingWrites();
     
    long transferFrom(FileChannel fileChannel, long position, long count) throws IOException;
}
