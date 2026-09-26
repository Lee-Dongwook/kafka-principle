package io.github.kafkaprinciple.common.utils.internals;

import java.nio.ByteBuffer;

public class SingleByteBufferOutputStream extends ByteBufferOutputStream {
    private static final float REALLOCATION_FACTOR = 1.1f;

    private final int initialCapacity;
    private final int initialPosition;
    private ByteBuffer buffer;

    public SingleByteBufferOutputStream(ByteBuffer buffer) {
        this.buffer = buffer;
        this.initialPosition = buffer.position();
        this.initialCapacity = buffer.capacity();
    }

    public SingleByteBufferOutputStream(int initialCapacity) {
        this(initialCapacity, false);
    }

    public SingleByteBufferOutputStream(int initialCapacity, boolean directBuffer) {
        this(directBuffer ? ByteBuffer.allocateDirect(initialCapacity) : ByteBuffer.allocate(initialCapacity));
    }

    @Override
    public void write(int b) {
        ensureRemaining(1);
        buffer.put((byte) b);
    }

    @Override
    public void write(byte[] bytes, int off, int len) {
        ensureRemaining(len);
        buffer.put(bytes, off, len);
    }

    @Override
    public void write(ByteBuffer sourceBuffer) {
        ensureRemaining(sourceBuffer.remaining());
        buffer.put(sourceBuffer);
    }

    @Override
    public ByteBuffer buffer() {
        return buffer;
    }

    @Override
    public int position() {
        return buffer.position();
    }

    @Override
    public int remaining() {
        return buffer.remaining();
    }

    public int limit() {
        return buffer.limit();
    }

    @Override
    public void position(int position) {
        ensureRemaining(position - buffer.position());
        buffer.position(position);
    }

    @Override
    public int initialCapacity() {
        return initialCapacity;
    }

    public void ensureRemaining(int remainingBytesRequired) {
        if (remainingBytesRequired > buffer.remaining())
            expandBuffer(remainingBytesRequired);
    }

    private void expandBuffer(int remainingRequired) {
        int expandSize = Math.max((int) (buffer.limit() * REALLOCATION_FACTOR), buffer.position() + remainingRequired);
        ByteBuffer temp = ByteBuffer.allocate(expandSize);
        int limit = limit();
        buffer.flip();
        temp.put(buffer);
        buffer.limit(limit);
        buffer.position(initialPosition);
        buffer = temp;
    }
}
