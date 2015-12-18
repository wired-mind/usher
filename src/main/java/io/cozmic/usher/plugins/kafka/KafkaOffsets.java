package io.cozmic.usher.plugins.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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
import java.util.concurrent.atomic.AtomicReference;

/**
 * KafkaOffsets
 * Created by Craig Earley on 9/18/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaOffsets {
    public static final String DEFAULT_CLIENT_NAME = KafkaOffsets.class.getSimpleName();
    private static final Logger logger = LoggerFactory.getLogger(KafkaOffsets.class.getName());
    //
    // CorrelationId - This is a user-supplied integer. It will be passed back
    // in the response by the server, unmodified. It is useful for matching request
    // and response between the client and server.
    //
    // See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Requests
    //
    private static final AtomicInteger correlationId = new AtomicInteger(0);
    private static final int READ_TIMEOUT_MS = 5_000; // channel read timeout in millis
    private static final short VERSION_ID = 1; // version 1 and above commit to Kafka, version 0 commits to ZooKeeper
    private static final String DEFAULT_COMMIT_METADATA = "";
    private final Object lockObject = new Object();
    private final Vertx vertx;
    private final String groupId;
    private final List<String> brokers;
    private final String clientName;
    private final AtomicReference<BlockingChannel> channel = new AtomicReference<>();

    public KafkaOffsets(Vertx vertx, List<String> brokers, String groupId) {
        this(vertx, brokers, groupId, DEFAULT_CLIENT_NAME);
    }

    public KafkaOffsets(Vertx vertx, List<String> brokers, String groupId, String clientName) {
        Preconditions.checkNotNull(vertx, "vertx cannot be null");
        Preconditions.checkArgument(brokers != null && brokers.size() > 0, "one or more brokers must be provided");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(groupId), "must provide a groupId");
        this.vertx = vertx;
        this.groupId = groupId;
        this.brokers = brokers;
        this.clientName = clientName;
    }

    private void connectToOffsetManager() throws ConsumerOffsetsException {
        if (channel.get() != null && channel.get().isConnected()) {
            return;
        }
        synchronized (lockObject) {
            for (final String broker : brokers) {
                final String[] strings = broker.split(":");

                // Create blocking channel with read timeout
                channel.set(new BlockingChannel(strings[0], Integer.valueOf(strings[1]),
                        BlockingChannel.UseDefaultBufferSize(),
                        BlockingChannel.UseDefaultBufferSize(),
                        READ_TIMEOUT_MS));
                try {
                    channel.get().connect();

                    channel.get().send(new ConsumerMetadataRequest(groupId, ConsumerMetadataRequest.CurrentVersion(), correlationId.getAndIncrement(), clientName));
                    final ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.get().receive().buffer());

                    if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
                        final Broker offsetManager = metadataResponse.coordinator();
                        // if the coordinator is different from the above channel's host (and port) then reconnect
                        if (!(offsetManager.host().equals(channel.get().host())
                                && offsetManager.port() == channel.get().port())) {
                            channel.get().disconnect();
                            channel.set(new BlockingChannel(offsetManager.host(), offsetManager.port(),
                                    BlockingChannel.UseDefaultBufferSize(),
                                    BlockingChannel.UseDefaultBufferSize(),
                                    READ_TIMEOUT_MS));
                            channel.get().connect();
                        }
                        return;
                    } else {
                        throw new ConsumerOffsetsException("Error in ConsumerMetadataResponse", metadataResponse.errorCode());
                    }
                } catch (ConsumerOffsetsException e) {
                    if (channel.get().isConnected()) {
                        channel.get().disconnect();
                    }
                    if (brokers.indexOf(broker) < brokers.size() - 1) {
                        continue;
                    }
                    throw e;
                    // Client should retry (after backoff)
                } catch (Exception e) {
                    if (channel.get().isConnected()) {
                        channel.get().disconnect();
                    }
                    if (brokers.indexOf(broker) < brokers.size() - 1) {
                        continue;
                    }
                    throw new ConsumerOffsetsException(e);
                    // Client should retry query (after backoff)
                }
            }
        }
    }

    public void commitOffsets(final Map<TopicAndPartition, Long> offsets, Handler<AsyncResult<Void>> resultHandler) {
        vertx.executeBlocking(offsetFuture -> {
            try {
                doCommitOffsets(offsets);
                offsetFuture.complete();
            } catch (Throwable t) {
                offsetFuture.fail(t);
            }
        }, true, resultHandler);

    }

    private void doCommitOffsets(Map<TopicAndPartition, Long> offsets) throws ConsumerOffsetsException {
        connectToOffsetManager();

        final long now = System.currentTimeMillis();
        final Map<TopicAndPartition, OffsetAndMetadata> requestInfo = new LinkedHashMap<>();
        offsets.forEach((topicAndPartition, offset) -> requestInfo.put(topicAndPartition, new OffsetAndMetadata(offset, DEFAULT_COMMIT_METADATA, now)));
        final OffsetCommitRequest commitRequest = new OffsetCommitRequest(
                groupId,
                requestInfo,
                correlationId.getAndIncrement(),
                clientName,
                VERSION_ID);
        try {
            channel.get().send(commitRequest.underlying());
            final OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(channel.get().receive().buffer());

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
                if (channel.get().isConnected()) {
                    channel.get().disconnect();
                }
            }
            throw e;
        } catch (Exception e) {
            if (channel.get().isConnected()) {
                channel.get().disconnect();
            }
            throw new ConsumerOffsetsException(e);
        }
    }

    public void commitOffset(final TopicAndPartition topicAndPartition, final Long offset, Handler<AsyncResult<Void>> resultHandler) {
        commitOffsets(ImmutableMap.of(topicAndPartition, offset), resultHandler);
    }

    public long getOffset(final TopicAndPartition topicAndPartition) throws ConsumerOffsetsException {
        connectToOffsetManager();

        final List<TopicAndPartition> partitions = new ArrayList<>();
        partitions.add(topicAndPartition);
        final OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                groupId,
                partitions,
                VERSION_ID,
                correlationId.incrementAndGet(),
                clientName + "_" + topicAndPartition.topic() + "_" + topicAndPartition.partition());
        try {
            channel.get().send(fetchRequest.underlying());
            final OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.get().receive().buffer());
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
                channel.get().disconnect();
                // Don't throw error but retry the fetch (with limited retries)
            }
            throw e;
        } catch (Throwable t) {
            channel.get().disconnect();
            throw new ConsumerOffsetsException(t);
            // Client should retry the commit
        }
    }

    public void shutdown(Handler<AsyncResult<Void>> stopHandler) {
        vertx.executeBlocking(future -> {
            try {
                if (channel.get() != null) {
                    channel.get().disconnect();
                }

                future.complete();
            } catch (Throwable t) {
                future.fail(t);
            }
        }, false, stopHandler);

    }
}
