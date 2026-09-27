package io.github.kafkaprinciple.server.log.remote.storage;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.TopicIdPartition;
import io.github.kafkaprinciple.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;

import java.util.Objects;
import java.util.Optional;

@InterfaceAudience.Public
public class RemoteLogSegmentMetadataUpdate extends RemoteLogMetadata {
    private final RemoteLogSegmentId remoteLogSegmentId;

    private final Optional<CustomMetadata> customMetadata;

    private final RemoteLogSegmentState state;

    public RemoteLogSegmentMetadataUpdate(RemoteLogSegmentId remoteLogSegmentId, long eventTimestampMs,
                                          Optional<CustomMetadata> customMetadata,
                                          RemoteLogSegmentState state,
                                          int brokerId) {
        super(brokerId, eventTimestampMs);
        this.remoteLogSegmentId = Objects.requireNonNull(remoteLogSegmentId, "remoteLogSegmentId can not be null");
        this.customMetadata = Objects.requireNonNull(customMetadata, "customMetadata can not be null");
        this.state = Objects.requireNonNull(state, "state can not be null");
    }

    public RemoteLogSegmentId remoteLogSegmentId() {
        return remoteLogSegmentId;
    }

    public Optional<CustomMetadata> customMetadata() {
        return customMetadata;
    }

    public RemoteLogSegmentState state() {
        return state;
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
        RemoteLogSegmentMetadataUpdate that = (RemoteLogSegmentMetadataUpdate) o;
        return Objects.equals(remoteLogSegmentId, that.remoteLogSegmentId) &&
               Objects.equals(customMetadata, that.customMetadata) &&
               state == that.state &&
               eventTimestampMs() == that.eventTimestampMs() &&
               brokerId() == that.brokerId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegmentId, customMetadata, state, eventTimestampMs(), brokerId());
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentMetadataUpdate{" +
               "remoteLogSegmentId=" + remoteLogSegmentId +
               ", customMetadata=" + customMetadata +
               ", state=" + state +
               ", eventTimestampMs=" + eventTimestampMs() +
               ", brokerId=" + brokerId() +
               '}';
    }
}
