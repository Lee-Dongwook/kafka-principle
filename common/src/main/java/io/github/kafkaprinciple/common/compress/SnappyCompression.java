package io.github.kafkaprinciple.common.compress;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.github.kafkaprinciple.common.errors.KafkaException;
import io.github.kafkaprinciple.common.record.internal.CompressionType;
import io.github.kafkaprinciple.common.utils.internals.BufferSupplier;
import io.github.kafkaprinciple.common.utils.internals.ByteBufferOutputStream;
import io.github.kafkaprinciple.common.utils.internals.ChunkedBytesStream;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class SnappyCompression implements Compression {
    private SnappyCompression() {
    }

    @Override
    public CompressionType type() {
        return CompressionType.SNAPPY;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        try {
            return new SnappyOutputStream(bufferStream);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion,
            BufferSupplier decompressionBufferSupplier) {
        try {
            return new ChunkedBytesStream(new SnappyInputStream(new ByteBufferInputStream(buffer)),
                    decompressionBufferSupplier,
                    decompressionOutputSize(),
                    false);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SnappyCompression;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public static class Builder implements Compression.Builder<SnappyCompression> {

        @Override
        public SnappyCompression build() {
            return new SnappyCompression();
        }
    }
}
