package io.github.kafkaprinciple.common.utils.internals;


import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public abstract class BufferSupplier implements AutoCloseable {
    public static final BufferSupplier NO_CACHING = new BufferSupplier() {
        @Override 
        public ByteBuffer get(int capacity) {
            return ByteBuffer.allocate(capacity);
        }

        @Override
        public void release(ByteBuffer buffer) {}

        @Override
        public void close() {}
    };

    public static BufferSupplier create() {
        return new DefaultSupplier();
    }

    public abstract ByteBuffer get(int capacity);

    public abstract void release(ByteBuffer buffer);
    
    public abstract void close();

    private static class DefaultSupplier extends BufferSupplier {
        private final Map<Integer, Deque<ByteBuffer>> bufferMap = new HashMap<>(1);

        @Override 
        public ByteBuffer get(int size) {
            Deque<ByteBuffer> bufferQueue = bufferMap.get(size);
            if (bufferQueue == null || bufferQueue.isEmpty())
                return ByteBuffer.allocate(size);
            else
                return bufferQueue.pollFirst();
        }

        @Override 
        public void release(ByteBuffer buffer) {
            buffer.clear();
            Deque<ByteBuffer> bufferQueue = bufferMap.computeIfAbsent(buffer.capacity(), k -> new ArrayDeque<>(1));
            bufferQueue.addLast(buffer);
        }

        @Override 
        public void close() {
            bufferMap.clear();
        }
    }

}
