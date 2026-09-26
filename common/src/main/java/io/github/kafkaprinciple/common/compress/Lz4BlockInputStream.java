package io.github.kafkaprinciple.common.compress;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import io.github.kafkaprinciple.common.utils.internals.BufferSupplier;
import io.github.kafkaprinciple.common.compress.Lz4BlockOutputStream.BD;
import io.github.kafkaprinciple.common.compress.Lz4BlockOutputStream.FLG;
import static io.github.kafkaprinciple.common.compress.Lz4BlockOutputStream.LZ4_FRAME_INCOMPRESSIBLE_MASK;
import static io.github.kafkaprinciple.common.compress.Lz4BlockOutputStream.MAGIC;

public final class Lz4BlockInputStream extends InputStream {
    public static final String PREMATURE_EOS = "Stream ended prematurely";
    public static final String NOT_SUPPORTED = "Stream unsupported (invalid magic bytes)";
    public static final String BLOCK_HASH_MISMATCH = "Block checksum mismatch";
    public static final String DESCRIPTOR_HASH_MISMATCH = "Stream frame descriptor corrupted";

    private static final LZ4SafeDecompressor DECOMPRESSOR = LZ4Factory.fastestInstance().safeDecompressor();
    private static final XXHash32 CHECKSUM = XXHashFactory.fastestInstance().hash32();

    private static final RuntimeException BROKEN_LZ4_EXCEPTION;

    static {
        RuntimeException exception = null;
        try {
            detectBrokenLz4Version();
        } catch (RuntimeException e) {
            exception = e;
        }
        BROKEN_LZ4_EXCEPTION = exception;
    }

    private final ByteBuffer in;
    private final boolean ignoreFlagDescriptorChecksum;
    private final BufferSupplier bufferSupplier;
    private final ByteBuffer decompressionBuffer;

    private FLG flg;
    private int maxBlockSize;

    private ByteBuffer decompressedBuffer;
    private boolean finished;

    public Lz4BlockInputStream(ByteBuffer in, BufferSupplier bufferSupplier, boolean ignoreFlagDescriptorChecksum)
            throws IOException {
        if (BROKEN_LZ4_EXCEPTION != null) {
            throw BROKEN_LZ4_EXCEPTION;
        }
        this.ignoreFlagDescriptorChecksum = ignoreFlagDescriptorChecksum;
        this.in = in.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        this.bufferSupplier = bufferSupplier;
        readHeader();
        decompressionBuffer = bufferSupplier.get(maxBlockSize);
        finished = false;
    }

    public boolean ignoreFlagDescriptorChecksum() {
        return this.ignoreFlagDescriptorChecksum;
    }

    private void readHeader() throws IOException {
        if (in.remaining() < 6) {
            throw new IOException(PREMATURE_EOS);
        }

        if (MAGIC != in.getInt()) {
            throw new IOException(NOT_SUPPORTED);
        }
        in.mark();

        flg = FLG.fromByte(in.get());
        maxBlockSize = BD.fromByte(in.get()).getBlockMaximumSize();

        if (flg.isContentSizeSet()) {
            if (in.remaining() < 8) {
                throw new IOException(PREMATURE_EOS);
            }
            in.position(in.position() + 8);
        }

        if (ignoreFlagDescriptorChecksum) {
            in.position(in.position() + 1);
            return;
        }

        int len = in.position() - in.reset().position();

        int hash = CHECKSUM.hash(in, in.position(), len, 0);
        in.position(in.position() + len);
        if (in.get() != (byte) ((hash >> 8) & 0xFF)) {
            throw new IOException(DESCRIPTOR_HASH_MISMATCH);
        }
    }

    private void readBlock() throws IOException {
        if (in.remaining() < 4) {
            throw new IOException(PREMATURE_EOS);
        }

        int blockSize = in.getInt();
        boolean compressed = (blockSize & LZ4_FRAME_INCOMPRESSIBLE_MASK) == 0;
        blockSize &= ~LZ4_FRAME_INCOMPRESSIBLE_MASK;

        // Check for EndMark
        if (blockSize == 0) {
            finished = true;
            if (flg.isContentChecksumSet())
                in.getInt(); // TODO: verify this content checksum
            return;
        } else if (blockSize > maxBlockSize) {
            throw new IOException(String.format("Block size %d exceeded max: %d", blockSize, maxBlockSize));
        }

        if (in.remaining() < blockSize) {
            throw new IOException(PREMATURE_EOS);
        }

        if (compressed) {
            try {
                final int bufferSize = DECOMPRESSOR.decompress(in, in.position(), blockSize, decompressionBuffer, 0,
                        maxBlockSize);
                decompressionBuffer.position(0);
                decompressionBuffer.limit(bufferSize);
                decompressedBuffer = decompressionBuffer;
            } catch (LZ4Exception e) {
                throw new IOException(e);
            }
        } else {
            decompressedBuffer = in.slice();
            decompressedBuffer.limit(blockSize);
        }

        if (flg.isBlockChecksumSet()) {
            int hash = CHECKSUM.hash(in, in.position(), blockSize, 0);
            in.position(in.position() + blockSize);
            if (hash != in.getInt()) {
                throw new IOException(BLOCK_HASH_MISMATCH);
            }
        } else {
            in.position(in.position() + blockSize);
        }
    }

    @Override
    public int read() throws IOException {
        if (finished) {
            return -1;
        }
        if (available() == 0) {
            readBlock();
        }
        if (finished) {
            return -1;
        }

        return decompressedBuffer.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        net.jpountz.util.SafeUtils.checkRange(b, off, len);
        if (finished) {
            return -1;
        }
        if (available() == 0) {
            readBlock();
        }
        if (finished) {
            return -1;
        }
        len = Math.min(len, available());

        decompressedBuffer.get(b, off, len);
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        if (finished) {
            return 0;
        }
        if (available() == 0) {
            readBlock();
        }
        if (finished) {
            return 0;
        }
        int skipped = (int) Math.min(n, available());
        decompressedBuffer.position(decompressedBuffer.position() + skipped);
        return skipped;
    }

    @Override
    public int available() {
        return decompressedBuffer == null ? 0 : decompressedBuffer.remaining();
    }

    @Override
    public void close() {
        bufferSupplier.release(decompressionBuffer);
    }

    @Override
    public void mark(int readlimit) {
        throw new RuntimeException("mark not supported");
    }

    @Override
    public void reset() {
        throw new RuntimeException("reset not supported");
    }

    static void detectBrokenLz4Version() {
        byte[] source = new byte[] { 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3 };
        final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

        final byte[] compressed = new byte[compressor.maxCompressedLength(source.length)];
        final int compressedLength = compressor.compress(source, 0, source.length, compressed, 0,
                compressed.length);
        final byte[] zeroes = { 0, 0, 0, 0, 0 };
        ByteBuffer nonZeroOffsetBuffer = ByteBuffer
                .allocate(zeroes.length + compressed.length)
                .put(zeroes)
                .slice()
                .put(compressed);

        ByteBuffer dest = ByteBuffer.allocate(source.length);
        try {
            DECOMPRESSOR.decompress(nonZeroOffsetBuffer, 0, compressedLength, dest, 0, source.length);
        } catch (Exception e) {
            throw new RuntimeException("Kafka has detected a buggy lz4-java library (< 1.4.x) on the classpath."
                    + " If you are using Kafka client libraries, make sure your application does not"
                    + " accidentally override the version provided by Kafka or include multiple versions"
                    + " of the library on the classpath. The lz4-java version on the classpath should"
                    + " match the version the Kafka client libraries depend on. Adding -verbose:class"
                    + " to your JVM arguments may help understand which lz4-java version is getting loaded.", e);
        }
    }
}
