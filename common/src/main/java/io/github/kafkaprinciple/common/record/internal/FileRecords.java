package io.github.kafkaprinciple.common.record.internal;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.kafkaprinciple.common.errors.KafkaException;
import io.github.kafkaprinciple.common.network.TransferableChannel;
import io.github.kafkaprinciple.common.record.internal.FileLogInputStream.FileChannelRecordBatch;
import io.github.kafkaprinciple.common.utils.Utils;
import io.github.kafkaprinciple.common.utils.internals.AbstractIterator;
import io.github.kafkaprinciple.common.utils.internals.BufferSupplier;
import io.github.kafkaprinciple.common.utils.internals.CloseableIterator;

public class FileRecords extends AbstractRecords implements Closeable {
    private final boolean isSlice;
    private final int start;
    private final int end;

    private final Iterable<FileLogInputStream.FileChannelRecordBatch> batches;

    private final AtomicInteger size;
    private final FileChannel channel;
    private volatile File file;

    FileRecords(
            File file,
            FileChannel channel,
            int end) throws IOException {
        this.file = file;
        this.channel = channel;
        this.start = 0;
        this.end = end;
        this.isSlice = false;

        if (channel.size() > Integer.MAX_VALUE) {
            throw new KafkaException(
                    "The size of segment " + file + " (" + channel.size() +
                            ") is larger than the maximum allowed segment size of " + Integer.MAX_VALUE);
        }

        int limit = Math.min((int) channel.size(), end);
        this.size = new AtomicInteger(limit - start);

        channel.position(limit);
        batches = batchesFrom(start);
    }

    private FileRecords(
            File file,
            FileChannel channel,
            int start,
            int end) {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = true;

        this.size = new AtomicInteger(end - start);

        batches = batchesFrom(start);
    }

    @Override
    public int sizeInBytes() {
        return size.get();
    }

    public File file() {
        return file;
    }

    public FileChannel channel() {
        return channel;
    }

    public void readInto(ByteBuffer buffer, int position) throws IOException {
        Utils.readFully(channel, buffer, position + this.start);
        buffer.flip();
    }

    @Override
    public FileRecords slice(int position, int size) {
        int availableBytes = availableBytes(position, size);
        int startPosition = this.start + position;

        return new FileRecords(file, channel, startPosition, startPosition + availableBytes);
    }

    public UnalignedFileRecords sliceUnaligned(int position, int size) {
        int availableBytes = availableBytes(position, size);
        return new UnalignedFileRecords(channel, this.start + position, availableBytes);
    }

    private int availableBytes(int position, int size) {
        int currentSizeInBytes = sizeInBytes();

        if (position < 0)
            throw new IllegalArgumentException("Invalid position: " + position + " in read from " + this);
        if (position > currentSizeInBytes)
            throw new IllegalArgumentException("Slice from position " + position + " exceeds end position of " + this);
        if (size < 0)
            throw new IllegalArgumentException("Invalid size: " + size + " in read from " + this);

        int end = this.start + position + size;
        if (end < 0 || end > start + currentSizeInBytes)
            end = this.start + currentSizeInBytes;
        return end - (this.start + position);
    }

    public int append(MemoryRecords records) throws IOException {
        if (records.sizeInBytes() > Integer.MAX_VALUE - size.get())
            throw new IllegalArgumentException("Append of size " + records.sizeInBytes() +
                    " bytes is too large for segment with current file position at " + size.get());

        int written = records.writeFullyTo(channel);
        size.getAndAdd(written);
        return written;
    }

    public void flush() throws IOException {
        channel.force(true);
    }

    public void close() throws IOException {
        channel.close();
    }

    public boolean deleteIfExists() throws IOException {
        Utils.closeQuietly(channel, "FileChannel");
        return Files.deleteIfExists(file.toPath());
    }

    public void updateParentDir(File parentDir) {
        this.file = new File(parentDir, file.getName());
    }

    public void renameTo(File f) throws IOException {
        try {
            Utils.atomicMoveWithFallback(file.toPath(), f.toPath(), false);
        } finally {
            this.file = f;
        }
    }

    public int truncateTo(int targetSize) throws IOException {
        int originalSize = sizeInBytes();
        if (targetSize > originalSize || targetSize < 0)
            throw new KafkaException(
                    "Attempt to truncate log segment " + file + " to " + targetSize + " bytes failed, " +
                            " size of this log segment is " + originalSize + " bytes.");
        if (targetSize < (int) channel.size()) {
            channel.truncate(targetSize);
            size.set(targetSize);
        }
        return originalSize - targetSize;
    }

    @Override
    public int writeTo(TransferableChannel destChannel, int offset, int length) throws IOException {
        long newSize = Math.min(channel.size(), end) - start;
        int oldSize = sizeInBytes();
        if (newSize < oldSize)
            throw new KafkaException(String.format(
                    "Size of FileRecords %s has been truncated during write: old size %d, new size %d",
                    file.getAbsolutePath(), oldSize, newSize));

        long position = start + offset;
        int count = Math.min(length, oldSize - offset);
        return (int) destChannel.transferFrom(channel, position, count);
    }

    public LogOffsetPosition searchForOffsetFromPosition(long targetOffset, int startingPosition) {
        FileChannelRecordBatch prevBatch = null;

        for (FileChannelRecordBatch batch : batchesFrom(startingPosition)) {
            if (batch.baseOffset() == targetOffset) {
                return LogOffsetPosition.fromBatch(batch);
            }

            if (batch.baseOffset() > targetOffset) {
                if (prevBatch != null && prevBatch.lastOffset() >= targetOffset)
                    return LogOffsetPosition.fromBatch(prevBatch);

                else {
                    return LogOffsetPosition.fromBatch(batch);
                }
            }
            prevBatch = batch;
        }

        if (prevBatch != null && prevBatch.lastOffset() >= targetOffset)
            return LogOffsetPosition.fromBatch(prevBatch);

        return null;
    }

    public TimestampAndOffset searchForTimestamp(long targetTimestamp, int startingPosition, long startingOffset,
            int maxRecordBodySize) {
        for (RecordBatch batch : batchesFrom(startingPosition)) {
            if (batch.maxTimestamp() >= targetTimestamp) {
                try (CloseableIterator<Record> iterator = batch.streamingIterator(BufferSupplier.NO_CACHING,
                        maxRecordBodySize)) {
                    while (iterator.hasNext()) {
                        Record record = iterator.next();
                        long timestamp = record.timestamp();
                        if (timestamp >= targetTimestamp && record.offset() >= startingOffset)
                            return new TimestampAndOffset(timestamp, record.offset(),
                                    maybeLeaderEpoch(batch.partitionLeaderEpoch()));
                    }
                }
            }
        }
        return null;
    }

    public TimestampAndOffset largestTimestampAfter(int startingPosition) {
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        long shallowOffsetOfMaxTimestamp = -1L;
        int leaderEpochOfMaxTimestamp = RecordBatch.NO_PARTITION_LEADER_EPOCH;

        for (RecordBatch batch : batchesFrom(startingPosition)) {
            long timestamp = batch.maxTimestamp();
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
                shallowOffsetOfMaxTimestamp = batch.lastOffset();
                leaderEpochOfMaxTimestamp = batch.partitionLeaderEpoch();
            }
        }
        return new TimestampAndOffset(maxTimestamp, shallowOffsetOfMaxTimestamp,
                maybeLeaderEpoch(leaderEpochOfMaxTimestamp));
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }

    @Override
    public Iterable<FileChannelRecordBatch> batches() {
        return batches;
    }

    @Override
    public String toString() {
        return "FileRecords(size=" + sizeInBytes() +
                ", file=" + file +
                ", start=" + start +
                ", end=" + end +
                ")";
    }

    public Iterable<FileChannelRecordBatch> batchesFrom(final int start) {
        return () -> batchIterator(start);
    }

    @Override
    public AbstractIterator<FileChannelRecordBatch> batchIterator() {
        return batchIterator(start);
    }

    private AbstractIterator<FileChannelRecordBatch> batchIterator(int start) {
        final int end;
        if (isSlice)
            end = this.end;
        else
            end = this.sizeInBytes();
        FileLogInputStream inputStream = new FileLogInputStream(this, start, end);
        return new RecordBatchIterator<>(inputStream);
    }

    public static FileRecords open(File file,
            boolean mutable,
            boolean fileAlreadyExists,
            int initFileSize,
            boolean preallocate) throws IOException {
        FileChannel channel = openChannel(file, mutable, fileAlreadyExists, initFileSize, preallocate);
        int end = (!fileAlreadyExists && preallocate) ? 0 : Integer.MAX_VALUE;
        return new FileRecords(file, channel, end);
    }

    public static FileRecords open(File file,
            boolean fileAlreadyExists,
            int initFileSize,
            boolean preallocate) throws IOException {
        return open(file, true, fileAlreadyExists, initFileSize, preallocate);
    }

    public static FileRecords open(File file, boolean mutable) throws IOException {
        return open(file, mutable, false, 0, false);
    }

    public static FileRecords open(File file) throws IOException {
        return open(file, true);
    }

    private static FileChannel openChannel(File file,
            boolean mutable,
            boolean fileAlreadyExists,
            int initFileSize,
            boolean preallocate) throws IOException {
        if (mutable) {
            if (fileAlreadyExists || !preallocate) {
                return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
            } else {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                try {
                    randomAccessFile.setLength(initFileSize);
                    return randomAccessFile.getChannel();
                } catch (IOException e) {
                    randomAccessFile.close();
                    throw e;
                }
            }
        } else {
            return FileChannel.open(file.toPath());
        }
    }

    public static class LogOffsetPosition {
        public final long offset;
        public final int position;
        public final int size;

        public static LogOffsetPosition fromBatch(FileChannelRecordBatch batch) {
            return new LogOffsetPosition(batch.baseOffset(), batch.position(), batch.sizeInBytes());
        }

        public LogOffsetPosition(long offset, int position, int size) {
            this.offset = offset;
            this.position = position;
            this.size = size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            LogOffsetPosition that = (LogOffsetPosition) o;

            return offset == that.offset &&
                    position == that.position &&
                    size == that.size;

        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(offset);
            result = 31 * result + position;
            result = 31 * result + size;
            return result;
        }

        @Override
        public String toString() {
            return "LogOffsetPosition(" +
                    "offset=" + offset +
                    ", position=" + position +
                    ", size=" + size +
                    ')';
        }
    }

    public static class TimestampAndOffset {
        public final long timestamp;
        public final long offset;
        public final Optional<Integer> leaderEpoch;

        public TimestampAndOffset(long timestamp, long offset, Optional<Integer> leaderEpoch) {
            this.timestamp = timestamp;
            this.offset = offset;
            this.leaderEpoch = leaderEpoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TimestampAndOffset that = (TimestampAndOffset) o;
            return timestamp == that.timestamp &&
                    offset == that.offset &&
                    Objects.equals(leaderEpoch, that.leaderEpoch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, offset, leaderEpoch);
        }

        @Override
        public String toString() {
            return "TimestampAndOffset(" +
                    "timestamp=" + timestamp +
                    ", offset=" + offset +
                    ", leaderEpoch=" + leaderEpoch +
                    ')';
        }
    }
}
