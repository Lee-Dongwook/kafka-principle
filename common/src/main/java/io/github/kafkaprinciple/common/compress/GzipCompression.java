package io.github.kafkaprinciple.common.compress;

import io.github.kafkaprinciple.common.errors.KafkaException;
import io.github.kafkaprinciple.common.record.internal.CompressionType;
import io.github.kafkaprinciple.common.utils.internals.BufferSupplier;
import io.github.kafkaprinciple.common.utils.internals.ByteBufferInputStream;
import io.github.kafkaprinciple.common.utils.internals.ByteBufferOutputStream;
import io.github.kafkaprinciple.common.utils.internals.ChunkedBytesStream;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static io.github.kafkaprinciple.common.record.internal.CompressionType.GZIP;

public class GzipCompression implements Compression {
    private final int level;

    private GzipCompression(int level) {
        this.level = level;
    }

    @Override
    public CompressionType type() {
        return GZIP;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
        try {
            return new BufferedOutputStream(new GzipOutputStream(buffer, 8 * 1024, level), 16 * 1024);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion,
            BufferSupplier decompressionBufferSupplier) {
        try {
            return new ChunkedBytesStream(new GZIPInputStream(new ByteBufferInputStream(buffer), 8 * 1024),
                    decompressionBufferSupplier,
                    decompressionOutputSize(),
                    false);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
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
        GzipCompression that = (GzipCompression) o;
        return level == that.level;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level);
    }

    public static class Builder implements Compression.Builder<GzipCompression> {
        private int level = GZIP.defaultLevel();

        public Builder level(int level) {
            if ((level < GZIP.minLevel() || GZIP.maxLevel() < level) && level != GZIP.defaultLevel()) {
                throw new IllegalArgumentException("gzip doesn't support given compression level: " + level);
            }

            this.level = level;
            return this;
        }

        @Override
        public GzipCompression build() {
            return new GzipCompression(level);
        }
    }
}
