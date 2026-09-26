package io.github.kafkaprinciple.common.compress;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.github.kafkaprinciple.common.record.internal.CompressionType;
import io.github.kafkaprinciple.common.utils.internals.BufferSupplier;
import io.github.kafkaprinciple.common.utils.internals.ByteBufferOutputStream;

public interface Compression {
    CompressionType type();

    OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion);

    InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompresBufferSupplier);

    default int decompressionOutputSize() {
        throw new UnsupportedOperationException(
                "Size of decompression buffer is not defined for this compression type=" + type().name);
    }

    interface Builder<T extends Compression> {
        T build();
    }

    static Builder<? extends Compression> of(final String compressionName) {
        CompressionType compressionType = CompressionType.forName(compressionName);
        return of(compressionType);
    }

    static Builder<? extends Compression> of(final CompressionType compressionType) {
        switch (compressionType) {
            case NONE:
                return none();
            case GZIP:
                return gzip();
            case SNAPPY:
                return snappy();
            case LZ4:
                return lz4();
            case ZSTD:
                return zstd();
            default:
                throw new IllegalArgumentException("Unknown compression type: " + compressionType.name);
        }
    }

    NoCompression NONE = none().build();

    static NoCompression.Builder none() {
        return new NoCompression.Builder();
    }

    static GzipCompression.Builder gzip() {
        return new GzipCompression.Builder();
    }

    static SnappyCompression.Builder snappy() {
        return new SnappyCompression.Builder();
    }

    static Lz4Compression.Builder lz4() {
        return new Lz4Compression.Builder();
    }

    static ZstdCompression.Builder zstd() {
        return new ZstdCompression.Builder();
    }
}
