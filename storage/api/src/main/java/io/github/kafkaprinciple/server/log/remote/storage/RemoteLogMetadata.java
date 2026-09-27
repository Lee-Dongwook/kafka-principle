package io.github.kafkaprinciple.server.log.remote.storage;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.TopicIdPartition;

@InterfaceAudience.Public
public abstract class RemoteLogMetadata {
    private final int brokerId;

    private final long eventTimestampMs;

    protected RemoteLogMetadata(int brokerId, long eventTimestampMs) {
        this.brokerId = brokerId;
        this.eventTimestampMs = eventTimestampMs;
    }

    public long eventTimestampMs() {
        return eventTimestampMs;
    }

    public int brokerId() {
        return brokerId;
    }

    public abstract TopicIdPartition topicIdPartition();
}
