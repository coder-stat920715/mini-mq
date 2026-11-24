package io.mq.client;

import io.mq.domain.TopicPartition;

import java.util.Collection;

public interface RebalanceListener {
    /** Called before partitions are taken away; the consumer commits right after (when auto-commit is on). */
    default void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
    /** Called after the new assignment is known and positions are initialised. */
    default void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

    RebalanceListener NOOP = new RebalanceListener() {};
}
