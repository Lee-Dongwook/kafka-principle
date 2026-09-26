package io.github.kafkaprinciple.common.compress;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.github.kafkaprinciple.common.record.internal.CompressionType;
import io.github.kafkaprinciple.common.utils.internals.BufferSupplier;
import io.github.kafkaprinciple.common.utils.internals.ByteBufferInputStream;
import io.github.kafkaprinciple.common.utils.internals.ByteBufferOutputStream;

public class NoCompression implements Compression {
    private NoCompression() {
    }

    @Override
    public CompressionType type() {
        return CompressionType.NONE;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        return bufferStream;
    }

    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion,
            BufferSupplier decompressionBufferSupplier) {
        return new ByteBufferInputStream(buffer);
    }

    public static class Builder implements Compression.Builder<NoCompression> {

        @Override
        public NoCompression build() {
            return new NoCompression();
        }
    }
}
