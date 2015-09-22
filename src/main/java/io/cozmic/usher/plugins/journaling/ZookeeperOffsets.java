package io.cozmic.usher.plugins.journaling;

import com.google.common.collect.ImmutableMap;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.HashMap;
import java.util.Map;

/**
 * ZookeeperOffsets
 * Created by Craig Earley on 9/19/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class ZookeeperOffsets implements ConsumerOffsetsStrategy {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerImpl.class.getName());
    private static final String CLIENT = KafkaConsumerImpl.class.getSimpleName();

    private final SimpleConsumer consumer;

    private ZookeeperOffsets() {
        this.consumer = null;
    }

    public ZookeeperOffsets(SimpleConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void commitOffsets(Map<TopicAndPartition, Long> offsets) throws ConsumerOffsetsException {
        throw new ConsumerOffsetsException("Not implemented");
    }

    @Override
    public void commitOffset(TopicAndPartition topicAndPartition, Long offset) throws ConsumerOffsetsException {
        commitOffsets(ImmutableMap.of(topicAndPartition, offset));
    }

    @Override
    public long getOffset(TopicAndPartition topicAndPartition) throws ConsumerOffsetsException {
        final String topic = topicAndPartition.topic();
        final int partition = topicAndPartition.partition();
        final String clientName = CLIENT + "_" + topic + "_" + partition;

        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();

        // Kafka includes two constants: kafka.api.OffsetRequest.EarliestTime()
        // finds the beginning of the data in the logs and starts streaming from there,
        // kafka.api.OffsetRequest.LatestTime() will only stream new messages.
        // Don’t assume that offset 0 is the beginning offset, since messages age
        // out of the log over time.
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            String message = "Error fetching offset data";
            logger.error(message);
            throw new ConsumerOffsetsException(message, response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()));
        }
        long[] offsets = response.offsets(topicAndPartition.topic(), topicAndPartition.partition());
        return offsets[0];
    }
}
