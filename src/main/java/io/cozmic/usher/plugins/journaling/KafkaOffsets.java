package io.cozmic.usher.plugins.journaling;

import com.google.common.collect.ImmutableMap;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
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
public class KafkaOffsets implements ConsumerOffsetsStrategy {
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
    private static final short VERSION_ID = 0; // version 1 and above commit to Kafka, version 0 commits to ZooKeeper
    private static final String DEFAULT_COMMIT_METADATA = "";
    private final Object lockObject = new Object();
    private final String host;
    private final int port;
    private final String groupId;
    private BlockingChannel channel;

    private KafkaOffsets() {
        this.host = null;
        this.port = 0;
        this.groupId = null;
    }

    public KafkaOffsets(String host, int port, String groupId) {
        this.host = host;
        this.port = port;
        this.groupId = groupId;
    }

    private void connectToOffsetManager() throws ConsumerOffsetsException {
        synchronized (lockObject) {
            if (channel != null && channel.isConnected()) {
                return;
            }

            // Create blocking channel with read timeout
            channel = new BlockingChannel(host, port,
                    BlockingChannel.UseDefaultBufferSize(),
                    BlockingChannel.UseDefaultBufferSize(),
                    READ_TIMEOUT_MS);
            try {
                channel.connect();

                channel.send(new ConsumerMetadataRequest(groupId, ConsumerMetadataRequest.CurrentVersion(), correlationId.getAndIncrement(), CLIENT));
                ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

                if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
                    Broker offsetManager = metadataResponse.coordinator();
                    // if the coordinator is different from the above channel's host then reconnect
                    if (!offsetManager.host().equals(channel.host())) {
                        channel.disconnect();
                        channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                                BlockingChannel.UseDefaultBufferSize(),
                                BlockingChannel.UseDefaultBufferSize(),
                                READ_TIMEOUT_MS);
                        channel.connect();
                    }
                } else {
                    throw new ConsumerOffsetsException("Error in ConsumerMetadataResponse", metadataResponse.errorCode());
                }
            } catch (ConsumerOffsetsException e) {
                channel.disconnect();
                throw e;
            } catch (Throwable t) {
                channel.disconnect();
                throw new ConsumerOffsetsException(t);
            }
        }
    }

    @Override
    public void commitOffsets(Map<TopicAndPartition, Long> offsets) throws ConsumerOffsetsException {
        connectToOffsetManager();

        long now = System.currentTimeMillis();
        Map<TopicAndPartition, OffsetAndMetadata> requestInfo = new LinkedHashMap<>();
        offsets.forEach((topicAndPartition, offset) -> requestInfo.put(topicAndPartition, new OffsetAndMetadata(offset, DEFAULT_COMMIT_METADATA, now)));
        OffsetCommitRequest commitRequest = new OffsetCommitRequest(
                groupId,
                requestInfo,
                correlationId.getAndIncrement(),
                CLIENT,
                VERSION_ID);
        try {
            channel.send(commitRequest.underlying());
            OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(channel.receive().buffer());
            // TODO: commitResponse.errors() <=> commitResponse.hasError()?
            if (commitResponse.hasError()) {
                for (Object partitionErrorCode : commitResponse.errors().values()) {
                    Short errorCode = (Short) partitionErrorCode;
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
            }
            throw e;
        } catch (Exception e) {
            channel.disconnect();
            throw new ConsumerOffsetsException(e);
        }
    }

    @Override
    public void commitOffset(TopicAndPartition topicAndPartition, Long offset) throws ConsumerOffsetsException {
        commitOffsets(ImmutableMap.of(topicAndPartition, offset));
    }

    @Override
    public long getOffset(TopicAndPartition topicAndPartition) throws ConsumerOffsetsException {
        long retrievedOffset = -1L;

        connectToOffsetManager();

        List<TopicAndPartition> partitions = new ArrayList<>();
        partitions.add(topicAndPartition);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                groupId,
                partitions,
                VERSION_ID,
                correlationId.getAndIncrement(),
                CLIENT + "_" + topicAndPartition.topic() + "_" + topicAndPartition.partition());
        try {
            channel.send(fetchRequest.underlying());
            OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer());
            OffsetMetadataAndError result = fetchResponse.offsets().get(topicAndPartition);
            short offsetFetchErrorCode = result.error();
            if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
                throw new ConsumerOffsetsException("Not coordinator for consumer - Retry the offset fetch", offsetFetchErrorCode);
            } else if (offsetFetchErrorCode == ErrorMapping.OffsetsLoadInProgressCode()) {
                throw new ConsumerOffsetsException("Offset load in progress - Retry the offset fetch", offsetFetchErrorCode);
            } else {
                // Success
                retrievedOffset = result.offset() + 1;
            }
        } catch (ConsumerOffsetsException e) {
            if (e.getErrorCode() == ErrorMapping.NotCoordinatorForConsumerCode()) {
                channel.disconnect();
            }
            throw e;
        } catch (Throwable t) {
            channel.disconnect();
            throw new ConsumerOffsetsException(t);
        }
        return retrievedOffset;
    }
}
