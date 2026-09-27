package io.github.kafkaprinciple.server.log.remote.storage;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.TopicIdPartition;
import io.github.kafkaprinciple.common.Configurable;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@InterfaceAudience.Public
public interface RemoteLogMetadataManager extends Configurable, Closeable {
    CompletableFuture<Void> addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException;

    CompletableFuture<Void> updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate)
            throws RemoteStorageException;
    
    Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                int epochForOffset,
                                                                long offset
    ) throws RemoteStorageException;

    Optional<Long> highestOffsetForEpoch(TopicIdPartition topicIdPartition,
                                         int leaderEpoch) throws RemoteStorageException;
    CompletableFuture<Void> putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata)
            throws RemoteStorageException;
    
    Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition)
            throws RemoteStorageException;
    
    Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition,
                                                             int leaderEpoch) throws RemoteStorageException;

    void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                      Set<TopicIdPartition> followerPartitions);
    
    void onStopPartitions(Set<TopicIdPartition> partitions);

    long remoteLogSize(TopicIdPartition topicIdPartition, int leaderEpoch) throws RemoteStorageException;

    default Optional<RemoteLogSegmentMetadata> nextSegmentWithTxnIndex(TopicIdPartition topicIdPartition,
                                                                       int epoch,
                                                                       long offset) throws RemoteStorageException {
        return remoteLogSegmentMetadata(topicIdPartition, epoch, offset);
    }

    default boolean isReady(TopicIdPartition topicIdPartition) {
        return true;
    }
}
