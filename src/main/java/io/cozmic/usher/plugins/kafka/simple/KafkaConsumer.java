package io.cozmic.usher.plugins.kafka.simple;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndOffset;

import java.io.Closeable;

/**
 * A Kafka client that consumes records from a Kafka cluster.
 * <p>
 * Created by Craig Earley on 9/16/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public interface KafkaConsumer extends Closeable {

    /**
     * Close this consumer.
     */
    void close();

    /**
     * Synchronously commits the specified offset for
     * the specified topic and partition to Kafka.
     *
     * @param topicAndPartition The topic and partition.
     * @param offset            The offset for the given topic and partition
     *                          that should be committed to Kafka.
     */
    void commit(TopicAndPartition topicAndPartition, Long offset) throws Exception;

    /**
     * Polls data for a topic and partition.
     *
     * @param topicAndPartition Topic and partition.
     * @return a MessageAndOffset object.
     */
    MessageAndOffset poll(TopicAndPartition topicAndPartition);
}
