package io.github.kafkaprinciple.common.compress;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.io.IOException;
import java.io.OutputStream;

import io.github.kafkaprinciple.common.record.internal.CompressionType;
import io.github.kafkaprinciple.common.utils.internals.ByteUtils;

public final class Lz4BlockOutputStream extends OutputStream {
    public static final int MAGIC = 0x184D2204;
    public static final int LZ4_FRAME_INCOMPRESSIBLE_MASK = 0x80000000;

    public static final String CLOSED_STREAM = "The stream is already closed";

    public static final int BLOCKSIZE_64KB = 4;

    private final LZ4Compressor compressor;
    private final XXHash32 checksum;
    private final boolean useBrokenFlagDescriptorChecksum;
    private final FLG flg;
    private final BD bd;
    private final int maxBlockSize;
    private OutputStream out;
    private byte[] buffer;
    private byte[] compressedBuffer;
    private int bufferOffset;
    private boolean finished;

    public Lz4BlockOutputStream(OutputStream out, int blockSize, int level, boolean blockChecksum,
            boolean useBrokenFlagDescriptorChecksum) throws IOException {
        this.out = out;
        compressor = level == CompressionType.LZ4.defaultLevel() ? LZ4Factory.fastestInstance().fastCompressor()
                : LZ4Factory.fastestInstance().highCompressor(level);
        checksum = XXHashFactory.fastestInstance().hash32();
        this.useBrokenFlagDescriptorChecksum = useBrokenFlagDescriptorChecksum;
        bd = new BD(blockSize);
        flg = new FLG(blockChecksum);
        bufferOffset = 0;
        maxBlockSize = bd.getBlockMaximumSize();
        buffer = new byte[maxBlockSize];
        compressedBuffer = new byte[compressor.maxCompressedLength(maxBlockSize)];
        finished = false;
        writeHeader();
    }

    public Lz4BlockOutputStream(OutputStream out, int level, boolean useBrokenFlagDescriptorChecksum)
            throws IOException {
        this(out, BLOCKSIZE_64KB, level, false, useBrokenFlagDescriptorChecksum);
    }

    public boolean useBrokenFlagDescriptorChecksum() {
        return this.useBrokenFlagDescriptorChecksum;
    }

    private void writeHeader() throws IOException {
        ByteUtils.writeUnsignedIntLE(buffer, 0, MAGIC);
        bufferOffset = 4;
        buffer[bufferOffset++] = flg.toByte();
        buffer[bufferOffset++] = bd.toByte();
        // TODO write uncompressed content size, update flg.validate()

        int offset = 4;
        int len = bufferOffset - offset;
        if (this.useBrokenFlagDescriptorChecksum) {
            len += offset;
            offset = 0;
        }
        byte hash = (byte) ((checksum.hash(buffer, offset, len, 0) >> 8) & 0xFF);
        buffer[bufferOffset++] = hash;

        out.write(buffer, 0, bufferOffset);
        bufferOffset = 0;
    }

    private void writeBlock() throws IOException {
        if (bufferOffset == 0) {
            return;
        }

        int compressedLength = compressor.compress(buffer, 0, bufferOffset, compressedBuffer, 0);
        byte[] bufferToWrite = compressedBuffer;
        int compressMethod = 0;

        if (compressedLength >= bufferOffset) {
            bufferToWrite = buffer;
            compressedLength = bufferOffset;
            compressMethod = LZ4_FRAME_INCOMPRESSIBLE_MASK;
        }

        ByteUtils.writeUnsignedIntLE(out, compressedLength | compressMethod);
        out.write(bufferToWrite, 0, compressedLength);

        if (flg.isBlockChecksumSet()) {
            int hash = checksum.hash(bufferToWrite, 0, compressedLength, 0);
            ByteUtils.writeUnsignedIntLE(out, hash);
        }
        bufferOffset = 0;
    }

    private void writeEndMark() throws IOException {
        ByteUtils.writeUnsignedIntLE(out, 0);
        // TODO implement content checksum, update flg.validate()
    }

    @Override
    public void write(int b) throws IOException {
        ensureNotFinished();
        if (bufferOffset == maxBlockSize) {
            writeBlock();
        }
        buffer[bufferOffset++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        net.jpountz.util.SafeUtils.checkRange(b, off, len);
        ensureNotFinished();

        int bufferRemainingLength = maxBlockSize - bufferOffset;
        while (len > bufferRemainingLength) {
            System.arraycopy(b, off, buffer, bufferOffset, bufferRemainingLength);
            bufferOffset = maxBlockSize;
            writeBlock();
            off += bufferRemainingLength;
            len -= bufferRemainingLength;
            bufferRemainingLength = maxBlockSize;
        }

        System.arraycopy(b, off, buffer, bufferOffset, len);
        bufferOffset += len;
    }

    @Override
    public void flush() throws IOException {
        if (!finished) {
            writeBlock();
        }
        if (out != null) {
            out.flush();
        }
    }

    private void ensureNotFinished() {
        if (finished) {
            throw new IllegalStateException(CLOSED_STREAM);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (!finished) {
                writeBlock();
                writeEndMark();
            }
        } finally {
            try {
                if (out != null) {
                    try (OutputStream outStream = out) {
                        outStream.flush();
                    }
                }
            } finally {
                out = null;
                buffer = null;
                compressedBuffer = null;
                finished = true;
            }
        }
    }

    public static class FLG {

        private static final int VERSION = 1;

        private final int reserved;
        private final int contentChecksum;
        private final int contentSize;
        private final int blockChecksum;
        private final int blockIndependence;
        private final int version;

        public FLG(boolean blockChecksum) {
            this(0, 0, 0, blockChecksum ? 1 : 0, 1, VERSION);
        }

        private FLG(int reserved,
                int contentChecksum,
                int contentSize,
                int blockChecksum,
                int blockIndependence,
                int version) {
            this.reserved = reserved;
            this.contentChecksum = contentChecksum;
            this.contentSize = contentSize;
            this.blockChecksum = blockChecksum;
            this.blockIndependence = blockIndependence;
            this.version = version;
            validate();
        }

        public static FLG fromByte(byte flg) {
            int reserved = (flg >>> 0) & 3;
            int contentChecksum = (flg >>> 2) & 1;
            int contentSize = (flg >>> 3) & 1;
            int blockChecksum = (flg >>> 4) & 1;
            int blockIndependence = (flg >>> 5) & 1;
            int version = (flg >>> 6) & 3;

            return new FLG(reserved,
                    contentChecksum,
                    contentSize,
                    blockChecksum,
                    blockIndependence,
                    version);
        }

        public byte toByte() {
            return (byte) (((reserved & 3) << 0) | ((contentChecksum & 1) << 2)
                    | ((contentSize & 1) << 3) | ((blockChecksum & 1) << 4) | ((blockIndependence & 1) << 5)
                    | ((version & 3) << 6));
        }

        private void validate() {
            if (reserved != 0) {
                throw new RuntimeException("Reserved bits must be 0");
            }
            if (blockIndependence != 1) {
                throw new RuntimeException("Dependent block stream is unsupported");
            }
            if (version != VERSION) {
                throw new RuntimeException(String.format("Version %d is unsupported", version));
            }
        }

        public boolean isContentChecksumSet() {
            return contentChecksum == 1;
        }

        public boolean isContentSizeSet() {
            return contentSize == 1;
        }

        public boolean isBlockChecksumSet() {
            return blockChecksum == 1;
        }

        public int getVersion() {
            return version;
        }
    }

    public static class BD {

        private final int reserved2;
        private final int blockSizeValue;
        private final int reserved3;

        public BD(int blockSizeValue) {
            this(0, blockSizeValue, 0);
        }

        private BD(int reserved2, int blockSizeValue, int reserved3) {
            this.reserved2 = reserved2;
            this.blockSizeValue = blockSizeValue;
            this.reserved3 = reserved3;
            validate();
        }

        public static BD fromByte(byte bd) {
            int reserved2 = (bd >>> 0) & 15;
            int blockMaximumSize = (bd >>> 4) & 7;
            int reserved3 = (bd >>> 7) & 1;

            return new BD(reserved2, blockMaximumSize, reserved3);
        }

        private void validate() {
            if (reserved2 != 0) {
                throw new RuntimeException("Reserved2 field must be 0");
            }
            if (blockSizeValue < 4 || blockSizeValue > 7) {
                throw new RuntimeException("Block size value must be between 4 and 7");
            }
            if (reserved3 != 0) {
                throw new RuntimeException("Reserved3 field must be 0");
            }
        }

        public int getBlockMaximumSize() {
            return 1 << ((2 * blockSizeValue) + 8);
        }

        public byte toByte() {
            return (byte) (((reserved2 & 15) << 0) | ((blockSizeValue & 7) << 4) | ((reserved3 & 1) << 7));
        }
    }
}
