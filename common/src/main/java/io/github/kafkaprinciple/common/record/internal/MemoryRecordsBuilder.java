package io.github.kafkaprinciple.common.record.internal;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.github.kafkaprinciple.common.compress.Compression;
import io.github.kafkaprinciple.common.errors.KafkaException;
import io.github.kafkaprinciple.common.header.Header;
import io.github.kafkaprinciple.common.message.KRaftVersionRecord;
import io.github.kafkaprinciple.common.record.TimestampType;
import io.github.kafkaprinciple.common.protocol.MessageUtil;
import io.github.kafkaprinciple.common.utils.Utils;
import io.github.kafkaprinciple.common.utils.internals.ByteBufferOutputStream;
import io.github.kafkaprinciple.common.utils.internals.SingleByteBufferOutputStream;

import static io.github.kafkaprinciple.common.utils.Utils.wrapNullable;

public class MemoryRecordsBuilder implements AutoCloseable {
    private static final float COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;
    private static final DataOutputStream CLOSED_STREAM = new DataOutputStream(new OutputStream() {
        @Override
        public void write(int b) {
            throw new IllegalStateException("MemoryRecordsBuilder is closed for record appends");
        }
    });

    private final TimestampType timestampType;
    private final Compression compression;
    private final ByteBufferOutputStream bufferStream;
    private final byte magic;
    private final int initialPosition;
    private final long baseOffset;
    private final long logAppendTime;
    private final boolean isControlBatch;
    private final int partitionLeaderEpoch;
    private final int writeLimit;
    private final int batchHeaderSizeInBytes;
    private final long deleteHorizonMs;
    private float estimatedCompressionRatio = 1.0F;
    private DataOutputStream appendStream;
    private boolean isTransactional;
    private long producerId;
    private short producerEpoch;
    private int baseSequence;
    private int uncompressedRecordsSizeInBytes;
    private int numRecords;
    private float actualCompressionRatio;
    private long maxTimestamp;
    private long offsetOfMaxTimestamp = -1;
    private Long lastOffset = null;
    private Long baseTimestamp = null;
    private MemoryRecords builtRecords;
    private boolean aborted = false;

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream,
            byte magic,
            Compression compression,
            TimestampType timestampType,
            long baseOffset,
            long logAppendTime,
            long producerId,
            short producerEpoch,
            int baseSequence,
            boolean isTransactional,
            boolean isControlBatch,
            int partitionLeaderEpoch,
            int writeLimit,
            long deleteHorizonMs) {
        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("TimestampType must be set for magic >= 0");
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            if (isTransactional)
                throw new IllegalArgumentException("Transactional records are not supported for magic " + magic);
            if (isControlBatch)
                throw new IllegalArgumentException("Control records are not supported for magic " + magic);
            if (compression.type() == CompressionType.ZSTD)
                throw new IllegalArgumentException("ZStandard compression is not supported for magic " + magic);
            if (deleteHorizonMs != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Delete horizon timestamp is not supported for magic " + magic);
        }
        this.magic = magic;
        this.timestampType = timestampType;
        this.compression = compression;
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.numRecords = 0;
        this.uncompressedRecordsSizeInBytes = 0;
        this.actualCompressionRatio = 1;
        this.maxTimestamp = RecordBatch.NO_TIMESTAMP;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
        this.isControlBatch = isControlBatch;
        this.deleteHorizonMs = deleteHorizonMs;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.writeLimit = writeLimit;
        this.initialPosition = bufferStream.position();
        this.batchHeaderSizeInBytes = AbstractRecords.recordBatchHeaderSizeInBytes(magic, compression.type());

        bufferStream.position(initialPosition + batchHeaderSizeInBytes);
        this.bufferStream = bufferStream;
        this.appendStream = new DataOutputStream(compression.wrapForOutput(this.bufferStream, magic));

        if (hasDeleteHorizonMs()) {
            this.baseTimestamp = deleteHorizonMs;
        }
    }

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream,
            byte magic,
            Compression compression,
            TimestampType timestampType,
            long baseOffset,
            long logAppendTime,
            long producerId,
            short producerEpoch,
            int baseSequence,
            boolean isTransactional,
            boolean isControlBatch,
            int partitionLeaderEpoch,
            int writeLimit) {
        this(bufferStream, magic, compression, timestampType, baseOffset, logAppendTime, producerId,
                producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch, writeLimit,
                RecordBatch.NO_TIMESTAMP);
    }

    public MemoryRecordsBuilder(ByteBuffer buffer,
            byte magic,
            Compression compression,
            TimestampType timestampType,
            long baseOffset,
            long logAppendTime,
            long producerId,
            short producerEpoch,
            int baseSequence,
            boolean isTransactional,
            boolean isControlBatch,
            int partitionLeaderEpoch,
            int writeLimit) {
        this(new SingleByteBufferOutputStream(buffer), magic, compression, timestampType, baseOffset, logAppendTime,
                producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch,
                writeLimit);
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public int initialCapacity() {
        return bufferStream.initialCapacity();
    }

    public ByteBufferOutputStream bufferStream() {
        return bufferStream;
    }

    public double compressionRatio() {
        return actualCompressionRatio;
    }

    public Compression compression() {
        return compression;
    }

    public boolean isControlBatch() {
        return isControlBatch;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    public final boolean hasDeleteHorizonMs() {
        return magic >= RecordBatch.MAGIC_VALUE_V2 && deleteHorizonMs >= 0L;
    }

    public MemoryRecords build() {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }
        close();
        return builtRecords;
    }

    public RecordsInfo info() {
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            if (compression.type() != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                return new RecordsInfo(logAppendTime, lastOffset);
            else
                return new RecordsInfo(logAppendTime, baseOffset);
        } else if (maxTimestamp == RecordBatch.NO_TIMESTAMP) {
            return new RecordsInfo(RecordBatch.NO_TIMESTAMP, -1);
        } else {
            if (compression.type() != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                return new RecordsInfo(maxTimestamp, lastOffset);
            else
                return new RecordsInfo(maxTimestamp, offsetOfMaxTimestamp);
        }
    }

    public int numRecords() {
        return numRecords;
    }

    public int uncompressedBytesWritten() {
        return uncompressedRecordsSizeInBytes + batchHeaderSizeInBytes;
    }

    public void setProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (isClosed()) {
            throw new IllegalStateException(
                    "Trying to set producer state of an already closed batch. This indicates a bug on the client.");
        }

        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }

    public void overrideLastOffset(long lastOffset) {
        if (builtRecords != null)
            throw new IllegalStateException("Cannot override the last offset after the records have been built");
        this.lastOffset = lastOffset;
    }

    public void closeForRecordAppends() {
        if (appendStream != CLOSED_STREAM) {
            try {
                appendStream.close();
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                appendStream = CLOSED_STREAM;
            }
        }
    }

    public void abort() {
        closeForRecordAppends();
        buffer().position(initialPosition);
        aborted = true;
    }

    public void reopenAndRewriteProducerState(long producerId, short producerEpoch, int baseSequence,
            boolean isTransactional) {
        if (aborted)
            throw new IllegalStateException("Should not reopen a batch which is already aborted.");
        builtRecords = null;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }

    public void close() {
        if (aborted)
            throw new IllegalStateException("Cannot close MemoryRecordsBuilder as it has already been aborted");

        if (builtRecords != null)
            return;

        validateProducerState();

        closeForRecordAppends();

        if (numRecords == 0L) {
            buffer().position(initialPosition);
            builtRecords = MemoryRecords.EMPTY;
        } else {
            if (magic > RecordBatch.MAGIC_VALUE_V1)
                this.actualCompressionRatio = (float) writeDefaultBatchHeader() / this.uncompressedRecordsSizeInBytes;
            else if (compression.type() != CompressionType.NONE)
                this.actualCompressionRatio = (float) writeLegacyCompressedWrapperHeader()
                        / this.uncompressedRecordsSizeInBytes;

            ByteBuffer buffer = buffer().duplicate();
            buffer.flip();
            buffer.position(initialPosition);
            builtRecords = MemoryRecords.readableRecords(buffer.slice());
        }
    }

    private void validateProducerState() {
        if (isTransactional && producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("Cannot write transactional messages without a valid producer ID");

        if (producerId != RecordBatch.NO_PRODUCER_ID) {
            if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH)
                throw new IllegalArgumentException("Invalid negative producer epoch");

            if (baseSequence < 0 && !isControlBatch)
                throw new IllegalArgumentException("Invalid negative sequence number used");

            if (magic < RecordBatch.MAGIC_VALUE_V2)
                throw new IllegalArgumentException("Idempotent messages are not supported for magic " + magic);
        }
    }

    private int writeDefaultBatchHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initialPosition);
        int size = pos - initialPosition;
        int writtenCompressed = size - DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        int offsetDelta = (int) (lastOffset - baseOffset);

        final long maxTimestamp;
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            maxTimestamp = logAppendTime;
        else
            maxTimestamp = this.maxTimestamp;

        DefaultRecordBatch.writeHeader(buffer, baseOffset, offsetDelta, size, magic, compression.type(), timestampType,
                baseTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch,
                hasDeleteHorizonMs(), partitionLeaderEpoch, numRecords);

        buffer.position(pos);
        return writtenCompressed;
    }

    private int writeLegacyCompressedWrapperHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initialPosition);

        int wrapperSize = pos - initialPosition - Records.LOG_OVERHEAD;
        int writtenCompressed = wrapperSize - LegacyRecord.recordOverhead(magic);
        AbstractLegacyRecordBatch.writeHeader(buffer, lastOffset, wrapperSize);

        long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
        LegacyRecord.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compression.type(),
                timestampType);

        buffer.position(pos);
        return writtenCompressed;
    }

    private void appendWithOffset(long offset, boolean isControlRecord, long timestamp, ByteBuffer key,
            ByteBuffer value, Header[] headers) {
        try {
            if (isControlRecord != isControlBatch)
                throw new IllegalArgumentException("Control records can only be appended to control batches");

            if (lastOffset != null && offset <= lastOffset)
                throw new IllegalArgumentException(String.format("Illegal offset %d following previous offset %d " +
                        "(Offsets must increase monotonically).", offset, lastOffset));

            if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Invalid negative timestamp " + timestamp);

            if (magic < RecordBatch.MAGIC_VALUE_V2 && headers != null && headers.length > 0)
                throw new IllegalArgumentException("Magic v" + magic + " does not support record headers");

            if (baseTimestamp == null)
                baseTimestamp = timestamp;

            if (magic > RecordBatch.MAGIC_VALUE_V1) {
                appendDefaultRecord(offset, timestamp, key, value, headers);
            } else {
                appendLegacyRecord(offset, timestamp, key, value, magic);
            }
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    public void appendWithOffset(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        appendWithOffset(offset, false, timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    public void appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        appendWithOffset(offset, false, timestamp, key, value, headers);
    }

    public void appendWithOffset(long offset, long timestamp, byte[] key, byte[] value) {
        appendWithOffset(offset, timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    public void appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value) {
        appendWithOffset(offset, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    public void appendWithOffset(long offset, SimpleRecord record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    public void appendControlRecordWithOffset(long offset, SimpleRecord record) {
        short typeId = ControlRecordType.parseTypeId(record.key());
        ControlRecordType type = ControlRecordType.fromTypeId(typeId);
        if (type == ControlRecordType.UNKNOWN)
            throw new IllegalArgumentException("Cannot append record with unknown control record type " + typeId);

        appendWithOffset(offset, true, record.timestamp(),
                record.key(), record.value(), record.headers());
    }

    public void append(long timestamp, ByteBuffer key, ByteBuffer value) {
        append(timestamp, key, value, Record.EMPTY_HEADERS);
    }

    public void append(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        appendWithOffset(nextSequentialOffset(), timestamp, key, value, headers);
    }

    public void append(long timestamp, byte[] key, byte[] value) {
        append(timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    public void append(long timestamp, byte[] key, byte[] value, Header[] headers) {
        append(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    public void append(SimpleRecord record) {
        appendWithOffset(nextSequentialOffset(), record);
    }

    public void appendControlRecord(long timestamp, ControlRecordType type, ByteBuffer value) {
        ByteBuffer key = type.recordKey();
        appendWithOffset(nextSequentialOffset(), true, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    public void appendEndTxnMarker(long timestamp, EndTransactionMarker marker) {
        if (producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("End transaction marker requires a valid producerId");
        if (!isTransactional)
            throw new IllegalArgumentException(
                    "End transaction marker depends on batch transactional flag being enabled");
        ByteBuffer value = marker.serializeValue();
        appendControlRecord(timestamp, marker.controlType(), value);
    }

    public void appendLeaderChangeMessage(long timestamp, LeaderChangeMessage leaderChangeMessage) {
        if (partitionLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) {
            throw new IllegalArgumentException("Partition leader epoch must be valid, but get " + partitionLeaderEpoch);
        }
        appendControlRecord(
                timestamp,
                ControlRecordType.LEADER_CHANGE,
                MessageUtil.toByteBufferAccessor(leaderChangeMessage, ControlRecordUtils.LEADER_CHANGE_CURRENT_VERSION)
                        .buffer());
    }

    public void appendSnapshotHeaderMessage(long timestamp, SnapshotHeaderRecord snapshotHeaderRecord) {
        appendControlRecord(
                timestamp,
                ControlRecordType.SNAPSHOT_HEADER,
                MessageUtil
                        .toByteBufferAccessor(snapshotHeaderRecord, ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION)
                        .buffer());
    }

    public void appendSnapshotFooterMessage(long timestamp, SnapshotFooterRecord snapshotHeaderRecord) {
        appendControlRecord(
                timestamp,
                ControlRecordType.SNAPSHOT_FOOTER,
                MessageUtil
                        .toByteBufferAccessor(snapshotHeaderRecord, ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION)
                        .buffer());
    }

    public void appendKRaftVersionMessage(long timestamp, KRaftVersionRecord kraftVersionRecord) {
        appendControlRecord(
                timestamp,
                ControlRecordType.KRAFT_VERSION,
                MessageUtil.toByteBufferAccessor(kraftVersionRecord, ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION)
                        .buffer());
    }

    public void appendVotersMessage(long timestamp, VotersRecord votersRecord) {
        appendControlRecord(
                timestamp,
                ControlRecordType.KRAFT_VOTERS,
                MessageUtil.toByteBufferAccessor(votersRecord, ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION)
                        .buffer());
    }

    public void appendUncheckedWithOffset(long offset, LegacyRecord record) {
        ensureOpenForRecordAppend();
        try {
            int size = record.sizeInBytes();
            AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

            ByteBuffer buffer = record.buffer().duplicate();
            appendStream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            recordWritten(offset, record.timestamp(), size + Records.LOG_OVERHEAD);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    public void appendUncheckedWithOffset(long offset, SimpleRecord record) throws IOException {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            int offsetDelta = (int) (offset - baseOffset);
            long timestamp = record.timestamp();
            if (baseTimestamp == null)
                baseTimestamp = timestamp;

            int sizeInBytes = DefaultRecord.writeTo(appendStream,
                    offsetDelta,
                    timestamp - baseTimestamp,
                    record.key(),
                    record.value(),
                    record.headers());
            recordWritten(offset, timestamp, sizeInBytes);
        } else {
            LegacyRecord legacyRecord = LegacyRecord.create(magic,
                    record.timestamp(),
                    Utils.toNullableArray(record.key()),
                    Utils.toNullableArray(record.value()));
            appendUncheckedWithOffset(offset, legacyRecord);
        }
    }

    public void append(Record record) {
        appendWithOffset(record.offset(), isControlBatch, record.timestamp(), record.key(), record.value(),
                record.headers());
    }

    public void appendWithOffset(long offset, Record record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    public void appendWithOffset(long offset, LegacyRecord record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value());
    }

    public void append(LegacyRecord record) {
        appendWithOffset(nextSequentialOffset(), record);
    }

    private void appendDefaultRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value,
            Header[] headers) throws IOException {
        ensureOpenForRecordAppend();
        int offsetDelta = (int) (offset - baseOffset);
        long timestampDelta = timestamp - baseTimestamp;
        int sizeInBytes = DefaultRecord.writeTo(appendStream, offsetDelta, timestampDelta, key, value, headers);
        recordWritten(offset, timestamp, sizeInBytes);
    }

    private long appendLegacyRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value, byte magic)
            throws IOException {
        ensureOpenForRecordAppend();

        int size = LegacyRecord.recordSize(magic, key, value);
        AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

        if (timestampType == TimestampType.LOG_APPEND_TIME)
            timestamp = logAppendTime;
        long crc = LegacyRecord.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);
        recordWritten(offset, timestamp, size + Records.LOG_OVERHEAD);
        return crc;
    }

    private long toInnerOffset(long offset) {
        if (magic > 0 && compression.type() != CompressionType.NONE)
            return offset - baseOffset;
        return offset;
    }

    private void recordWritten(long offset, long timestamp, int size) {
        if (numRecords == Integer.MAX_VALUE)
            throw new IllegalArgumentException(
                    "Maximum number of records per batch exceeded, max records: " + Integer.MAX_VALUE);
        if (offset - baseOffset > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Maximum offset delta exceeded, base offset: " + baseOffset +
                    ", last offset: " + offset);

        numRecords += 1;
        uncompressedRecordsSizeInBytes += size;
        lastOffset = offset;

        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
            offsetOfMaxTimestamp = offset;
        }
    }

    private void ensureOpenForRecordAppend() {
        if (appendStream == CLOSED_STREAM)
            throw new IllegalStateException(
                    "Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
    }

    private void ensureOpenForRecordBatchWrite() {
        if (isClosed())
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is closed");
        if (aborted)
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is aborted");
    }

    private int estimatedBytesWritten() {
        return estimatedBytesWritten(uncompressedRecordsSizeInBytes);
    }

    private int estimatedBytesWritten(int uncompressedSize) {
        if (compression.type() == CompressionType.NONE) {
            return batchHeaderSizeInBytes + uncompressedSize;
        } else {
            return batchHeaderSizeInBytes
                    + (int) (uncompressedSize * estimatedCompressionRatio * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
    }

    public int estimatedBytesWrittenAfter(byte[] key, byte[] value, Header[] headers) {
        ByteBuffer keyBuffer = wrapNullable(key);
        ByteBuffer valueBuffer = wrapNullable(value);
        final int recordSize;
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            recordSize = Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, keyBuffer, valueBuffer);
        } else {
            recordSize = DefaultRecord.recordSizeUpperBound(keyBuffer, valueBuffer, headers);
        }
        return estimatedBytesWritten(uncompressedRecordsSizeInBytes + recordSize);
    }

    public void setEstimatedCompressionRatio(float estimatedCompressionRatio) {
        this.estimatedCompressionRatio = estimatedCompressionRatio;
    }

    public boolean hasRoomFor(long timestamp, byte[] key, byte[] value, Header[] headers) {
        return hasRoomFor(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    public boolean hasRoomFor(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        if (isFull())
            return false;

        if (numRecords == 0)
            return true;

        final int recordSize;
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            recordSize = Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
        } else {
            int nextOffsetDelta = lastOffset == null ? 0 : (int) (lastOffset - baseOffset + 1);
            long timestampDelta = baseTimestamp == null ? 0 : timestamp - baseTimestamp;
            recordSize = DefaultRecord.sizeInBytes(nextOffsetDelta, timestampDelta, key, value, headers);
        }

        return this.writeLimit >= estimatedBytesWritten() + recordSize;
    }

    public boolean hasRoomFor(int estimatedRecordsSize) {
        if (isFull())
            return false;
        return this.writeLimit >= estimatedBytesWritten() + estimatedRecordsSize;
    }

    public int maxAllowedBytes() {
        return this.writeLimit - this.batchHeaderSizeInBytes;
    }

    public boolean isClosed() {
        return builtRecords != null;
    }

    public boolean isFull() {
        return appendStream == CLOSED_STREAM || (this.numRecords > 0 && this.writeLimit <= estimatedBytesWritten());
    }

    public int estimatedSizeInBytes() {
        return builtRecords != null ? builtRecords.sizeInBytes() : estimatedBytesWritten();
    }

    public byte magic() {
        return magic;
    }

    private long nextSequentialOffset() {
        return lastOffset == null ? baseOffset : lastOffset + 1;
    }

    public static class RecordsInfo {
        public final long maxTimestamp;
        public final long shallowOffsetOfMaxTimestamp;

        public RecordsInfo(long maxTimestamp,
                long shallowOffsetOfMaxTimestamp) {
            this.maxTimestamp = maxTimestamp;
            this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
        }
    }

    public long producerId() {
        return this.producerId;
    }

    public short producerEpoch() {
        return this.producerEpoch;
    }

    public int baseSequence() {
        return this.baseSequence;
    }
}
