package io.github.kafkaprinciple.common.compress;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import io.github.kafkaprinciple.common.errors.KafkaException;
import io.github.kafkaprinciple.common.record.internal.CompressionType;
import io.github.kafkaprinciple.common.record.internal.RecordBatch;
import io.github.kafkaprinciple.common.utils.internals.BufferSupplier;
import io.github.kafkaprinciple.common.utils.internals.ByteBufferOutputStream;
import io.github.kafkaprinciple.common.utils.internals.ChunkedBytesStream;

import static io.github.kafkaprinciple.common.record.internal.CompressionType.LZ4;

public class Lz4Compression implements Compression {

    private final int level;

    private Lz4Compression(int level) {
        this.level = level;
    }

    @Override
    public CompressionType type() {
        return LZ4;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
        try {
            return new Lz4BlockOutputStream(buffer, level, messageVersion == RecordBatch.MAGIC_VALUE_V0);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public InputStream wrapForInput(ByteBuffer inputBuffer, byte messageVersion,
            BufferSupplier decompressionBufferSupplier) {
        try {
            return new ChunkedBytesStream(
                    new Lz4BlockInputStream(inputBuffer, decompressionBufferSupplier,
                            messageVersion == RecordBatch.MAGIC_VALUE_V0),
                    decompressionBufferSupplier, decompressionOutputSize(), true);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public int decompressionOutputSize() {
        return 2 * 1024;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Lz4Compression that = (Lz4Compression) o;
        return level == that.level;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level);
    }

    public static class Builder implements Compression.Builder<Lz4Compression> {
        private int level = LZ4.defaultLevel();

        public Builder level(int level) {
            if (level < LZ4.minLevel() || LZ4.maxLevel() < level) {
                throw new IllegalArgumentException("lz4 doesn't support given compression level: " + level);
            }

            this.level = level;
            return this;
        }

        @Override
        public Lz4Compression build() {
            return new Lz4Compression(level);
        }
    }
}
