package io.github.kafkaprinciple.common.compress;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import io.github.kafkaprinciple.common.errors.KafkaException;
import io.github.kafkaprinciple.common.record.internal.CompressionType;
import io.github.kafkaprinciple.common.utils.internals.BufferSupplier;
import io.github.kafkaprinciple.common.utils.internals.ByteBufferInputStream;
import io.github.kafkaprinciple.common.utils.internals.ByteBufferOutputStream;
import io.github.kafkaprinciple.common.utils.internals.ChunkedBytesStream;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;

import static io.github.kafkaprinciple.common.record.internal.CompressionType.ZSTD;

public class ZstdCompression implements Compression {
    private final int level;

    private ZstdCompression(int level) {
        this.level = level;
    }

    @Override
    public CompressionType type() {
        return ZSTD;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        try {
            return new BufferedOutputStream(
                    new ZstdOutputStreamNoFinalizer(bufferStream, RecyclingBufferPool.INSTANCE, level), 16 * 1024);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion,
            BufferSupplier decompressionBufferSupplier) {
        try {
            return new ChunkedBytesStream(wrapForZstdInput(buffer, decompressionBufferSupplier),
                    decompressionBufferSupplier,
                    decompressionOutputSize(),
                    false);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    public static ZstdInputStreamNoFinalizer wrapForZstdInput(ByteBuffer buffer,
            BufferSupplier decompressionBufferSupplier) throws IOException {
        final BufferPool bufferPool = new BufferPool() {
            @Override
            public ByteBuffer get(int capacity) {
                return decompressionBufferSupplier.get(capacity);
            }

            @Override
            public void release(ByteBuffer buffer) {
                decompressionBufferSupplier.release(buffer);
            }
        };

        return new ZstdInputStreamNoFinalizer(new ByteBufferInputStream(buffer), bufferPool);
    }

    @Override
    public int decompressionOutputSize() {
        return 16 * 1024;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ZstdCompression that = (ZstdCompression) o;
        return level == that.level;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level);
    }

    public static class Builder implements Compression.Builder<ZstdCompression> {
        private int level = ZSTD.defaultLevel();

        public Builder level(int level) {
            if (level < ZSTD.minLevel() || ZSTD.maxLevel() < level) {
                throw new IllegalArgumentException("zstd doesn't support given compression level: " + level);
            }

            this.level = level;
            return this;
        }

        @Override
        public ZstdCompression build() {
            return new ZstdCompression(level);
        }
    }
}
