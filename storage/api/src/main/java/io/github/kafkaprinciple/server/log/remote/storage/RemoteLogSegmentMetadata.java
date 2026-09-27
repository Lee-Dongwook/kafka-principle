package io.github.kafkaprinciple.server.log.remote.storage;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.TopicIdPartition;

import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;

@InterfaceAudience.Public
public class RemoteLogSegmentMetadata extends RemoteLogMetadata {
    private final RemoteLogSegmentId remoteLogSegmentId;

    private final long startOffset;

    private final long endOffset;

    private final long maxTimestampMs;

    private final NavigableMap<Interger, Long> segmentLeaderEpochs;

    private final int segmentSizeInBytes;

    private final Optional<CustomMetadata> customMetadata;

    private final RemoteLogSegmentState state;

    private final boolean txnIdxEmpty;

    public RemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId,
                                    long startOffset,
                                    long endOffset,
                                    long maxTimestampMs,
                                    int brokerId,
                                    long eventTimestampMs,
                                    int segmentSizeInBytes,
                                    Optional<CustomMetadata> customMetadata,
                                    RemoteLogSegmentState state,
                                    Map<Integer, Long> segmentLeaderEpochs) {
        this(remoteLogSegmentId, startOffset, endOffset, maxTimestampMs, brokerId, eventTimestampMs, segmentSizeInBytes,
                customMetadata, state, segmentLeaderEpochs, false);
    }

    public RemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId,
                                    long startOffset,
                                    long endOffset,
                                    long maxTimestampMs,
                                    int brokerId,
                                    long eventTimestampMs,
                                    int segmentSizeInBytes,
                                    Optional<CustomMetadata> customMetadata,
                                    RemoteLogSegmentState state,
                                    Map<Integer, Long> segmentLeaderEpochs,
                                    boolean txnIdxEmpty) {
        super(brokerId, eventTimestampMs);
        this.remoteLogSegmentId = Objects.requireNonNull(remoteLogSegmentId, "remoteLogSegmentId can not be null");
        this.state = Objects.requireNonNull(state, "state can not be null");

        if (startOffset < 0) {
            throw new IllegalArgumentException("Unexpected start offset = " + startOffset + ". StartOffset for a remote segment cannot be negative");
        }
        this.startOffset = startOffset;

        if (endOffset < startOffset) {
            throw new IllegalArgumentException("Unexpected end offset = " + endOffset + 
                                               ". EndOffset for a remote segment cannot be less than startOffset = " + startOffset);
        }
        this.endOffset = endOffset;
        this.maxTimestampMs = maxTimestampMs;
        this.segmentSizeInBytes = segmentSizeInBytes;
        this.customMetadata = Objects.requireNonNull(customMetadata, "customMetadata can not be null");

        if (segmentLeaderEpochs == null || segmentLeaderEpochs.isEmpty()) {
            throw new IllegalArgumentException("segmentLeaderEpochs can not be null or empty");
        }

        this.segmentLeaderEpochs = Collections.unmodifiableNavigableMap(new TreeMap<>(segmentLeaderEpochs));
        this.txnIdxEmpty = txnIdxEmpty;
    }

    public RemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId,
                                    long startOffset,
                                    long endOffset,
                                    long maxTimestampMs,
                                    int brokerId,
                                    long eventTimestampMs,
                                    int segmentSizeInBytes,
                                    Map<Integer, Long> segmentLeaderEpochs) {
        this(remoteLogSegmentId,
                startOffset,
                endOffset,
                maxTimestampMs,
                brokerId,
                eventTimestampMs, segmentSizeInBytes,
                Optional.empty(),
                RemoteLogSegmentState.COPY_SEGMENT_STARTED,
                segmentLeaderEpochs);
    }

    public RemoteLogSegmentMetadata(RemoteLogSegmentId remoteLogSegmentId,
                                    long startOffset,
                                    long endOffset,
                                    long maxTimestampMs,
                                    int brokerId,
                                    long eventTimestampMs,
                                    int segmentSizeInBytes,
                                    Map<Integer, Long> segmentLeaderEpochs,
                                    boolean txnIdxEmpty) {
        this(remoteLogSegmentId, startOffset, endOffset, maxTimestampMs, brokerId, eventTimestampMs, segmentSizeInBytes,
                Optional.empty(), RemoteLogSegmentState.COPY_SEGMENT_STARTED, segmentLeaderEpochs, txnIdxEmpty);
    }

    public RemoteLogSegmentId remoteLogSegmentId() {
        return remoteLogSegmentId;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public int segmentSizeInBytes() {
        return segmentSizeInBytes;
    }

    public long maxTimestampMs() {
        return maxTimestampMs;
    }

    public NavigableMap<Integer, Long> segmentLeaderEpochs() {
        return segmentLeaderEpochs;
    }

    public Optional<CustomMetadata> customMetadata() {
        return customMetadata;
    }

    public RemoteLogSegmentState state() {
        return state;
    }

    public boolean isTxnIdxEmpty() {
        return txnIdxEmpty;
    }

    public RemoteLogSegmentMetadata createWithUpdates(RemoteLogSegmentMetadataUpdate rlsmUpdate) {
        if (!remoteLogSegmentId.equals(rlsmUpdate.remoteLogSegmentId())) {
            throw new IllegalArgumentException("Given rlsmUpdate does not have this instance's remoteLogSegmentId.");
        }

        return new RemoteLogSegmentMetadata(remoteLogSegmentId, startOffset,
                endOffset, maxTimestampMs, rlsmUpdate.brokerId(), rlsmUpdate.eventTimestampMs(),
                segmentSizeInBytes, rlsmUpdate.customMetadata(), rlsmUpdate.state(), segmentLeaderEpochs, txnIdxEmpty);
    }

    @Override
    public TopicIdPartition topicIdPartition() {
        return remoteLogSegmentId.topicIdPartition();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteLogSegmentMetadata that = (RemoteLogSegmentMetadata) o;
        return startOffset == that.startOffset && endOffset == that.endOffset
                && maxTimestampMs == that.maxTimestampMs
                && segmentSizeInBytes == that.segmentSizeInBytes
                && Objects.equals(remoteLogSegmentId, that.remoteLogSegmentId)
                && Objects.equals(segmentLeaderEpochs, that.segmentLeaderEpochs)
                && Objects.equals(customMetadata, that.customMetadata)
                && state == that.state
                && eventTimestampMs() == that.eventTimestampMs()
                && brokerId() == that.brokerId()
                && txnIdxEmpty == that.txnIdxEmpty;
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegmentId, startOffset, endOffset, brokerId(), maxTimestampMs,
                eventTimestampMs(), segmentLeaderEpochs, segmentSizeInBytes, customMetadata, state, txnIdxEmpty);
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentMetadata{" +
               "remoteLogSegmentId=" + remoteLogSegmentId +
               ", startOffset=" + startOffset +
               ", endOffset=" + endOffset +
               ", brokerId=" + brokerId() +
               ", maxTimestampMs=" + maxTimestampMs +
               ", eventTimestampMs=" + eventTimestampMs() +
               ", segmentLeaderEpochs=" + segmentLeaderEpochs +
               ", segmentSizeInBytes=" + segmentSizeInBytes +
               ", customMetadata=" + customMetadata +
               ", state=" + state +
               ", txnIdxEmpty=" + txnIdxEmpty +
               '}';
    }

    public static class CustomMetadata {
        private final byte[] value;

        public CustomMetadata(byte[] value) {
            this.value = value;
        }

        public byte[] value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CustomMetadata that = (CustomMetadata) o;
            return Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }

        @Override
        public String toString() {
            return "CustomMetadata{" + value.length + " bytes}";
        }
    }
}
