package io.cozmic.usher.plugins.kafka;

import com.google.common.collect.ImmutableMap;
import kafka.api.ConsumerMetadataRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.network.BlockingChannel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KafkaOffsets
 * Created by Craig Earley on 9/18/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaOffsets {
    private static final String CLIENT = KafkaOffsets.class.getSimpleName();
    /*
     * CorrelationId - This is a user-supplied integer. It will be passed back
     * in the response by the server, unmodified. It is useful for matching request
     * and response between the client and server.
     *
     * See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Requests
     */
    private static final AtomicInteger correlationId = new AtomicInteger(0);
    private static final int READ_TIMEOUT_MS = 5_000; // channel read timeout in millis
    private static final short VERSION_ID = 1; // version 1 and above commit to Kafka, version 0 commits to ZooKeeper
    private static final String DEFAULT_COMMIT_METADATA = "";
    private final Object lockObject = new Object();
    private final String groupId;
    private final List<String> brokers;
    private BlockingChannel channel;

    private KafkaOffsets() {
        this.groupId = null;
        this.brokers = null;
    }

    @Deprecated
    public KafkaOffsets(String host, int port, String groupId) {
        this.groupId = groupId;
        this.brokers = new ArrayList<>();
        this.brokers.add(String.format("%s:%d", host, port));
    }

    public KafkaOffsets(List<String> brokers, String groupId) {
        this.groupId = groupId;
        this.brokers = brokers;
    }

    private void connectToOffsetManager() throws ConsumerOffsetsException {
        synchronized (lockObject) {
            if (channel != null && channel.isConnected()) {
                return;
            }
            for (final String broker : brokers) {
                final String[] parts = broker.split(":");
                final String host = parts[0];
                final int port = Integer.valueOf(parts[1]);

                // Create blocking channel with read timeout
                channel = new BlockingChannel(host, port,
                        BlockingChannel.UseDefaultBufferSize(),
                        BlockingChannel.UseDefaultBufferSize(),
                        READ_TIMEOUT_MS);
                try {
                    channel.connect();

                    channel.send(new ConsumerMetadataRequest(groupId, ConsumerMetadataRequest.CurrentVersion(), correlationId.getAndIncrement(), CLIENT));
                    final ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

                    if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
                        final Broker offsetManager = metadataResponse.coordinator();
                        // if the coordinator is different from the above channel's host then reconnect
                        if (!offsetManager.host().equals(channel.host())) {
                            channel.disconnect();
                            channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                                    BlockingChannel.UseDefaultBufferSize(),
                                    BlockingChannel.UseDefaultBufferSize(),
                                    READ_TIMEOUT_MS);
                            channel.connect();
                        }
                        return;
                    } else {
                        throw new ConsumerOffsetsException("Error in ConsumerMetadataResponse", metadataResponse.errorCode());
                    }
                } catch (ConsumerOffsetsException e) {
                    channel.disconnect();
                    if (brokers.indexOf(broker) < brokers.size() - 1) {
                        continue;
                    }
                    throw e;
                    // TODO: Client should retry (after backoff)
                } catch (Exception e) {
                    channel.disconnect();
                    if (brokers.indexOf(broker) < brokers.size() - 1) {
                        continue;
                    }
                    throw new ConsumerOffsetsException(e);
                    // TODO: Client should retry query (after backoff)
                }
            }
        }
    }

    public void commitOffsets(final Map<TopicAndPartition, Long> offsets) throws ConsumerOffsetsException {
        connectToOffsetManager();

        final long now = System.currentTimeMillis();
        final Map<TopicAndPartition, OffsetAndMetadata> requestInfo = new LinkedHashMap<>();
        offsets.forEach((topicAndPartition, offset) -> requestInfo.put(topicAndPartition, new OffsetAndMetadata(offset, DEFAULT_COMMIT_METADATA, now)));
        final OffsetCommitRequest commitRequest = new OffsetCommitRequest(
                groupId,
                requestInfo,
                correlationId.getAndIncrement(),
                CLIENT,
                VERSION_ID);
        try {
            channel.send(commitRequest.underlying());
            final OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(channel.receive().buffer());

            if (commitResponse.hasError()) {
                for (Object partitionErrorCode : commitResponse.errors().values()) {
                    final Short errorCode = (Short) partitionErrorCode;
                    if (errorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
                        throw new RuntimeException("You must reduce the size of the metadata if you wish to retry");
                    } else if (errorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
                        throw new ConsumerOffsetsException("Not coordinator for consumer - Retry the commit", errorCode);
                    } else if (errorCode == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                        throw new ConsumerOffsetsException("Consumer coordinator not available - Retry the commit", errorCode);
                    } else {
                        throw new ConsumerOffsetsException("Error in OffsetCommitResponse", errorCode);
                    }
                }
            }
        } catch (ConsumerOffsetsException e) {
            if (e.getErrorCode() == ErrorMapping.NotCoordinatorForConsumerCode()
                    || e.getErrorCode() == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                channel.disconnect();
                // TODO: Don't throw error but retry the commit (with limited retries)
            }
            throw e;
        } catch (Exception e) {
            channel.disconnect();
            throw new ConsumerOffsetsException(e);
            // TODO: Client should retry the commit
        }
    }

    public void commitOffset(final TopicAndPartition topicAndPartition, final Long offset) throws ConsumerOffsetsException {
        commitOffsets(ImmutableMap.of(topicAndPartition, offset));
    }

    public long getOffset(final TopicAndPartition topicAndPartition) throws ConsumerOffsetsException {
        connectToOffsetManager();

        final List<TopicAndPartition> partitions = new ArrayList<>();
        partitions.add(topicAndPartition);
        final OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                groupId,
                partitions,
                VERSION_ID,
                correlationId.getAndIncrement(),
                CLIENT + "_" + topicAndPartition.topic() + "_" + topicAndPartition.partition());
        try {
            channel.send(fetchRequest.underlying());
            final OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer());
            final OffsetMetadataAndError result = fetchResponse.offsets().get(topicAndPartition);
            final short offsetFetchErrorCode = result.error();
            if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
                throw new ConsumerOffsetsException("Not coordinator for consumer - Retry the offset fetch", offsetFetchErrorCode);
            } else if (offsetFetchErrorCode == ErrorMapping.OffsetsLoadInProgressCode()) {
                throw new ConsumerOffsetsException("Offset load in progress - Retry the offset fetch", offsetFetchErrorCode);
            } else {
                // Success
                return result.offset();
                //String retrievedMetadata = result.metadata();
            }
        } catch (ConsumerOffsetsException e) {
            if (e.getErrorCode() == ErrorMapping.NotCoordinatorForConsumerCode()) {
                channel.disconnect();
                // TODO: Don't throw error but retry the fetch (with limited retries)
            }
            throw e;
        } catch (Throwable t) {
            channel.disconnect();
            throw new ConsumerOffsetsException(t);
            // TODO: Client should retry the commit
        }
    }
}
