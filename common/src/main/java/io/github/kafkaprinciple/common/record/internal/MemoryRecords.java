package io.github.kafkaprinciple.common.record.internal;

import io.github.kafkaprinciple.common.network.TransferableChannel;
import io.github.kafkaprinciple.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class MemoryRecords extends AbstractRecords {
    public static final MemoryRecords EMPTY = MemoryRecords.readableRecords(ByteBuffer.allocate(0));

    private final ByteBuffer buffer;

    private final Iterable<MutableRecordBatch> batches = this::batchIterator;

    private int validBytes = -1;

    private MemoryRecords(ByteBuffer buffer) {
        Objects.requireNonNull(buffer, "buffer should not be null");
        this.buffer = buffer;
    }

    @Override 
    public int sizeInBytes() {
        return buffer.limit();
    }

    @Override
    public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
        if (((long) position) + length > buffer.limit())
            throw new IllegalArgumentException("position+length should not be greater than buffer.limit(), position: "
                    + position + ", length: " + length + ", buffer.limit(): " + buffer.limit());

        return Utils.tryWriteTo(channel, position, length, buffer);
    }

    public int writeFullyTo(GatheringByteChannel channel) throws IOException {
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes())
            written += channel.write(buffer);
        buffer.reset();
        return written;
    }

    public int validBytes() {
        if (validBytes >= 0)
            return validBytes;

        int bytes = 0;
        for (RecordBatch batch : batches())
            bytes += batch.sizeInBytes();

        this.validBytes = bytes;
        return bytes;
    }

    @Override
    public AbstractIterator<MutableRecordBatch> batchIterator() {
        return new RecordBatchIterator<>(new ByteBufferLogInputStream(buffer.duplicate(), Integer.MAX_VALUE));
    }

    public Integer firstBatchSize() {
        if (buffer.remaining() < HEADER_SIZE_UP_TO_MAGIC)
            return null;
        return new ByteBufferLogInputStream(buffer, Integer.MAX_VALUE).nextBatchSize();
    }

    public FilterResult filterTo(RecordFilter filter, ByteBuffer destinationBuffer, int maxRecordBodySize) {
        FilterResult filterResult = new FilterResult(destinationBuffer);
        SingleByteBufferOutputStream bufferOutputStream = new SingleByteBufferOutputStream(destinationBuffer);

        for (MutableRecordBatch batch : batches()) {
            final BatchRetentionResult batchRetentionResult = filter.checkBatchRetention(batch);
            final boolean containsMarkerForEmptyTxn = batchRetentionResult.containsMarkerForEmptyTxn;
            final BatchRetention batchRetention = batchRetentionResult.batchRetention;

            filterResult.bytesRead += batch.sizeInBytes();

            if (batchRentention == BatchRetention.DELETE)
                continue;

            final BatchFilterResult iterationResult = filterBatch(batch, decompressionBufferSupplier, filterResult,
                        filter, maxRecordBodySize
            );

            List<Record> retainedRecords = iterationResult.retainedRecords;
            boolean containsTombstones = iterationResult.containsTombstones;
            boolean writeOriginalBatch = iterationResult.writeOriginalBatch;
            long maxOffset = iterationResult.maxOffset;

            if (!retainedRecords.isEmpty()) {
                boolean needToSetDeleteHorizon = (containsTombstones || containsMarkerForEmptyTxn) &&
                        batch.deleteHorizonMs().isEmpty();
                if (writeOriginalBatch && !needToSetDeleteHorizon) {
                    batch.writeTo(bufferOutputStream);
                    filterResult.updateRetainedBatchMetadata(batch, retainedRecords.size(), false);
                } else {
                    long deleteHorizonMs;
                    if (needToSetDeleteHorizon)
                        deleteHorizonMs = filter.currentTime + filter.deleteRetentionMs;
                    else 
                        deleteHorizonMs = batch.deleteHorizonMs().orElse(RecordBatch.NO_TIMESTAMP);
                    try (final MemoryRecordsBuilder builder = buildRentainedRecordsInto(batch, retainedRecords,
                                bufferOutputStream, deleteHorizonMs
                    )) {
                        //TODO
                    }
                }
            }
        }
    }

}
