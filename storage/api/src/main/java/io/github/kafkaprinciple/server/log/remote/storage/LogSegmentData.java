package io.github.kafkaprinciple.server.log.remote.storage;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

@InterfaceAudience.Public
public class LogSegmentData {
    private final Path logSegment;
    private final Path offsetIndex;
    private final Path timeIndex;
    private final Optional<Path> transactionIndex;
    private final Path producerSnapshotIndex;
    private final ByteBuffer leaderEpochIndex;

    public LogSegmentData(Path logSegment,
                          Path offsetIndex,
                          Path timeIndex,
                          Optional<Path> transactionIndex,
                          Path producerSnapshotIndex,
                          ByteBuffer leaderEpochIndex) {
        this.logSegment = Objects.requireNonNull(logSegment, "logSegment can not be null");
        this.offsetIndex = Objects.requireNonNull(offsetIndex, "offsetIndex can not be null");
        this.timeIndex = Objects.requireNonNull(timeIndex, "timeIndex can not be null");
        this.transactionIndex = Objects.requireNonNull(transactionIndex, "transactionIndex can not be null");
        this.producerSnapshotIndex = Objects.requireNonNull(producerSnapshotIndex, "producerSnapshotIndex can not be null");
        this.leaderEpochIndex = Objects.requireNonNull(leaderEpochIndex, "leaderEpochIndex can not be null");
    }

    public Path logSegment() {
        return logSegment;
    }

    public Path offsetIndex() {
        return offsetIndex;
    }

    public Path timeIndex() {
        return timeIndex;
    }

    public Optional<Path> transactionIndex() {
        return transactionIndex;
    }

    public Path producerSnapshotIndex() {
        return producerSnapshotIndex;
    }

    public ByteBuffer leaderEpochIndex() {
        return leaderEpochIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogSegmentData that = (LogSegmentData) o;
        return Objects.equals(logSegment, that.logSegment) &&
               Objects.equals(offsetIndex, that.offsetIndex) &&
               Objects.equals(timeIndex, that.timeIndex) &&
               Objects.equals(transactionIndex, that.transactionIndex) &&
               Objects.equals(producerSnapshotIndex, that.producerSnapshotIndex) &&
               Objects.equals(leaderEpochIndex, that.leaderEpochIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logSegment, offsetIndex, timeIndex, transactionIndex, producerSnapshotIndex, leaderEpochIndex);
    }

    @Override
    public String toString() {
        return "LogSegmentData{" +
               "logSegment=" + logSegment +
               ", offsetIndex=" + offsetIndex +
               ", timeIndex=" + timeIndex +
               ", txnIndex=" + transactionIndex +
               ", producerSnapshotIndex=" + producerSnapshotIndex +
               ", leaderEpochIndex=" + leaderEpochIndex +
               '}';
    }
}
