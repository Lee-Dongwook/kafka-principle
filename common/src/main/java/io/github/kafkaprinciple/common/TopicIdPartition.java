package io.github.kafkaprinciple.common;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.util.Objects;

@InterfaceAudience.Public
public class TopicIdPartition {
    private final Uuid topicId;
    private final TopicPartition topicPartition;

    public TopicIdPartition(Uuid topicId, TopicPartition topicPartition) {
        this.topicId = Objects.requireNonNull(topicId, "topicId can not be null");
        this.topicPartition = Objects.requireNonNull(topicPartition, "topicPartition can not be null");
    }

    public TopicIdPartition(Uuid topicId, int partition, String topic) {
        this.topicId = Objects.requireNonNull(topicId, "topicId can not be null");
        this.topicPartition = new TopicPartition(topic, partition);
    }

    public Uuid topicId() {
        return topicId;
    }

    public String topic() {
        return topicPartition.topic();
    }

    public int partition() {
        return topicPartition.partition();
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicIdPartition that = (TopicIdPartition) o;
        return topicId.equals(that.topicId) &&
                topicPartition.equals(that.topicPartition);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + topicId.hashCode();
        result = prime * result + topicPartition.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return topicId + ":" + topic() + "-" + partition();
    }
}
