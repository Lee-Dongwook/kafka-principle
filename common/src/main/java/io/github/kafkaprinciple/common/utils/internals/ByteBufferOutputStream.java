package io.github.kafkaprinciple.common.utils.internals;

import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class ByteBufferOutputStream extends OutputStream {

    @Override
    public abstract void write(int b);

    @Override
    public abstract void write(byte[] bytes, int off, int len);

    public abstract void write(ByteBuffer sourceBuffer);

    public abstract ByteBuffer buffer();

    public abstract int position();

    public abstract void position(int position);

    public abstract int remaining();

    public abstract int initialCapacity();
}
