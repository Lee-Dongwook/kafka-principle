package io.github.kafkaprinciple.common.protocol;

public class MessageSizeAccumulator {
    private int totalSize = 0;
    private int zeroCopySize = 0;

    public int totalSize() {
        return totalSize;
    }

    public int sizeExcludedZeroCopy() {
        return totalSize - zeroCopySize;
    }

    public void addZeroCopyBytes(int size) {
        zeroCopySize += size;
        totalSize += size;
    }

    public void addBytes(int size) {
        totalSize += size;
    }

    public void add(MessageSizeAccumulator size) {
        this.totalSize += size.totalSize;
        this.zeroCopySize += size.zeroCopySize;
    }
}
