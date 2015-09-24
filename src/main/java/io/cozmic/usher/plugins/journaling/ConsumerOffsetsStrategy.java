package io.cozmic.usher.plugins.journaling;

import kafka.common.TopicAndPartition;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.Map;

/**
 * ConsumerOffsetsStrategy
 * Created by Craig Earley on 9/18/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public interface ConsumerOffsetsStrategy {

    static ConsumerOffsetsStrategy createKafkaOffsetsStrategy(String host, int port, String groupId) {
        return new KafkaOffsets(host, port, groupId);
    }

    static ConsumerOffsetsStrategy createKafkaOffsetsStrategy(SimpleConsumer consumer) {
        return new ZookeeperOffsets(consumer);
    }

    /**
     * Commits the specified offsets for the specified list of topics and partitions.
     *
     * @param offsets The list of offsets per partition that should be committed.
     *                Throws an Exception if not successful.
     * @throws ConsumerOffsetsException
     */
    void commitOffsets(Map<TopicAndPartition, Long> offsets) throws ConsumerOffsetsException;

    /**
     * Commits the specified offset for the specified topic and partition.
     *
     * @param topicAndPartition The topic and partition.
     * @param offset            The offset for the given partition that should be committed.
     *                          Throws an Exception if not successful.
     * @throws ConsumerOffsetsException
     */
    void commitOffset(TopicAndPartition topicAndPartition, Long offset) throws ConsumerOffsetsException;

    /**
     * Fetches the offset for a topic and partition.
     *
     * @param topicAndPartition The partition for which the offset is returned.
     * @return The offset for the topic and partition.
     * @throws ConsumerOffsetsException
     */
    long getOffset(TopicAndPartition topicAndPartition) throws ConsumerOffsetsException;
}
