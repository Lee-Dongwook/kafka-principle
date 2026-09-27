package io.github.kafkaprinciple.clients.consumer;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.header.Headers;
import io.github.kafkaprinciple.common.header.internals.RecordHeaders;
import io.github.kafkaprinciple.common.record.TimestampType;
import io.github.kafkaprinciple.common.record.internal.RecordBatch;

import java.util.ConcurrentModificationException;
import java.util.Optional;

@InterfaceAudience.Public
public class ConsumerRecord<K,V> {
    public static final long NO_TIMESTAMP = RecordBatch.NO_TIMESTAMP;
    public static final int NULL_SIZE = -1;
    
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final TimestampType timestampType;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final Headers headers;
    private final K key;
    private final V value;
    private final Optional<Integer> leaderEpoch;
    private final Optional<Short> deliveryCount;

    public ConsumerRecord(String topic,
                          int partition,
                          long offset,
                          long timestamp,
                          TimestampType timestampType,
                          int serializedKeySize,
                          int serializedValueSize,
                          K key,
                          V value,
                          Headers headers,
                          Optional<Integer> leaderEpoch
    ) {
        this(topic, partition, offset, timestamp, timestampType, serializedKeySize, serializedValueSize,
            key, value, headers, leaderEpoch, Optional.empty()
        );
    }

    public ConsumerRecord(String topic,
                          int partition,
                          long offset,
                          long timestamp,
                          TimestampType timestampType,
                          int serializedKeySize,
                          int serializedValueSize,
                          K key,
                          V value,
                          Headers headers,
                          Optional<Integer> leaderEpoch,
                          Optional<Short> deliveryCount) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null");
        if (headers == null)
            throw new IllegalArgumentException("Headers cannot be null");

        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.leaderEpoch = leaderEpoch;
        this.deliveryCount = deliveryCount;
    }

    public String topic() {
        return this.topic;
    }

    public int partition() {
        return this.partition;
    }

    public Headers headers() {
        return headers;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    public long offset() {
        return offset;
    }

    public long timestamp() {
        return timestamp;
    }

    public TimestampType timestampType() {
        return timestampType;
    }

    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    public Optional<Integer> leaderEpoch() {
        return leaderEpoch;
    }

    public Optional<Short> deliveryCount() {
        return deliveryCount;
    }

    @Override
    public String toString() {
        return "ConsumerRecord(topic = " + topic
               + ", partition = " + partition
               + ", leaderEpoch = " + leaderEpoch.orElse(null)
               + ", offset = " + offset
               + ", " + timestampType + " = " + timestamp
               + ", deliveryCount = " + deliveryCount.orElse(null)
               + ", serialized key size = "  + serializedKeySize
               + ", serialized value size = " + serializedValueSize
               + ", headers = " + headers
               + ", key = " + key
               + ", value = " + value + ")";
    }
}
