package io.cozmic.usher.plugins.journaling;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndOffset;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

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
     * Asynchronously commits the specified offsets for
     * the specified list of topics and partitions to Kafka.
     * Can be made to wait (block) with commit(offsets).get().
     *
     * @param offsets The list of offsets per partition
     *                that should be committed to Kafka.
     * @return A Future which may contain an error if not successful.
     */
    @Deprecated
    java.util.concurrent.Future<AsyncResult<Void>> commit(Map<TopicAndPartition, Long> offsets);

    /**
     * Asynchronously commits the specified offsets for
     * the specified topic and partition to Kafka. Can
     * be made to wait (block) with commit(offset).get().
     *
     * @param topicAndPartition The topic and partition.
     * @param offset            The offset for the given topic and partition
     *                          that should be committed to Kafka.
     * @return A Future which may contain an error if not successful.
     */
    @Deprecated
    java.util.concurrent.Future<AsyncResult<Void>> commit(TopicAndPartition topicAndPartition, Long offset);

    /**
     * Asynchronously fetches data for the topics and partitions
     * specified using the subscribe API.
     *
     * @param asyncResultHandler A Handler with a map of topic to records since
     *                           the last fetch for the subscribed topics and partitions.
     */
    void poll(Handler<AsyncResult<Map<String, List<MessageAndOffset>>>> asyncResultHandler);

    /**
     * Asynchronously fetches data for a topic and partition.
     *
     * @param topicAndPartition  Topic and partition.
     * @param asyncResultHandler A Handler with a single entry map where its
     *                           key is the topic and its value is the list of records
     *                           since the last fetch for the given topic and partition.
     */
    void poll(TopicAndPartition topicAndPartition, Handler<AsyncResult<Map<String, List<MessageAndOffset>>>> asyncResultHandler);

    /**
     * Asynchronously fetches data for the topics and partitions
     * specified using the subscribe API. This method can be made
     * to wait (block) with poll().get(). (Note that poll().get()
     * will wait indefinitely if data is not available.)
     *
     * @return A Future with a map of topic to records since
     * the last fetch for the subscribed topics and partitions.
     */
    @Deprecated
    java.util.concurrent.Future<AsyncResult<Map<String, List<MessageAndOffset>>>> poll();

    /**
     * Asynchronously fetches data for a topic and partition.
     * This method can be made to wait (block) with
     * poll(topicAndPartition).get(). (Note that get()
     * will wait indefinitely if data is not available.)
     *
     * @param topicAndPartition Topic and partition.
     * @return A Future with a single entry map where its
     * key is the topic and its value is the list of records
     * since the last fetch for the given topic and partition.
     */
    @Deprecated
    java.util.concurrent.Future<AsyncResult<Map<String, List<MessageAndOffset>>>> poll(TopicAndPartition topicAndPartition);

    /**
     * Subscribes to a specific topic and partition.
     *
     * @param partitions Partitions to subscribe to.
     */
    void subscribe(TopicAndPartition... partitions);

    /**
     * Unsubscribe from the specific topic and partition.
     *
     * @param partitions Partitions to unsubscribe from.
     */
    void unsubscribe(TopicAndPartition... partitions);
}
