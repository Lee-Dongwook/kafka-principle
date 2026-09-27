package io.github.kafkaprinciple.server.log.remote.storage;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.Uuid;
import io.github.kafkaprinciple.common.TopicIdPartition;

import java.util.Objects;

@InterfaceAudience.Public
public class RemoteLogSegmentId {
    private final TopicIdPartition topicIdPartition;
    private final Uuid id;

    public static RemoteLogSegmentId generateNew(TopicIdPartition topicIdPartition) {
        return new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());
    }

    public RemoteLogSegmentId(TopicIdPartition topicIdPartition, Uuid id) {
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");
        this.id = Objects.requireNonNull(id, "id can not be null");
    }

    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    public Uuid id() {
        return id;
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentId{" +
               "topicIdPartition=" + topicIdPartition +
               ", id=" + id +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteLogSegmentId that = (RemoteLogSegmentId) o;
        return Objects.equals(topicIdPartition, that.topicIdPartition) && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicIdPartition, id);
    }
}
