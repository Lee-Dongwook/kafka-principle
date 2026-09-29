package io.github.kafkaprinciple.common.memory;

import java.nio.ByteBuffer;

public interface MemoryPool {
    MemoryPool NONE = new MemoryPool() {
        @Override
        public ByteBuffer tryAllocate(int sizeBytes) {
            return ByteBuffer.allocate(sizeBytes);
        }

        @Override
        public void release(ByteBuffer previouslyAllocated) {
            // nop
        }

        @Override
        public long size() {
            return Long.MAX_VALUE;
        }

        @Override
        public long availableMemory() {
            return Long.MAX_VALUE;
        }

        @Override
        public boolean isOutOfMemory() {
            return false;
        }

        @Override
        public String toString() {
            return "NONE";
        }
    };

    ByteBuffer tryAllocate(int sizeBytes);

    void release(ByteBuffer previouslyAllocated);

    long size();

    long availableMemory();

    boolean isOutOfMemory();
}
