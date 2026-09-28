package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.network.TransferableChannel;

import java.io.IOException;
import java.nio.channels.FileChannel;

public class UnalignedFileRecords implements UnalignedRecords {
    private final FileChannel channel;
    private final long position;
    private final int size;

    public UnalignedFileRecords(FileChannel channel, long position, int size) {
        this.channel = channel;
        this.position = position;
        this.size = size;
    }

    @Override
    public int sizeInBytes() {
        return size;
    }

    @Override
    public int writeTo(TransferableChannel destChannel, int previouslyWritten, int remaining) throws IOException {
        long position = this.position + previouslyWritten;
        int count = Math.min(remaining, sizeInBytes() - previouslyWritten);

        return (int) destChannel.transferFrom(channel, position, count);
    }
}
