package io.cozmic.usher.plugins.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KafkaOffsets commits offsets to (and fetches from) a durable Kafka
 * topic. Commits are regular producer requests (which are inexpensive)
 * and fetches are fast memory look-ups. This class is thread-safe.
 * <p>
 * Created by Craig Earley on 9/18/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaOffsets {
    public static final String CLIENT_NAME = KafkaOffsets.class.getName();
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
    private static CopyOnWriteArrayList<HostAndPort> brokers;
    private final Vertx vertx;
    private final String groupId;

    public KafkaOffsets(Vertx vertx, List<HostAndPort> brokers, String groupId) {
        Preconditions.checkNotNull(vertx, "vertx cannot be null");
        Preconditions.checkArgument(brokers != null && brokers.size() > 0, "one or more brokers must be provided");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(groupId), "must provide a groupId");
        this.vertx = vertx;
        this.groupId = groupId;
        KafkaOffsets.brokers = new CopyOnWriteArrayList<>(brokers);
    }

    private static BlockingChannel getConnectionToOffsetManager(String groupId) throws ConsumerOffsetsException {
        BlockingChannel channel = null;
        for (final HostAndPort broker : brokers) {

            // Create blocking channel with read timeout
            channel = new BlockingChannel(broker.getHost(), broker.getPort(),
                    BlockingChannel.UseDefaultBufferSize(),
                    BlockingChannel.UseDefaultBufferSize(),
                    READ_TIMEOUT_MS);
            try {
                channel.connect();

                channel.send(new ConsumerMetadataRequest(groupId, ConsumerMetadataRequest.CurrentVersion(), correlationId.getAndIncrement(), CLIENT_NAME));
                final ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

                if (metadataResponse.errorCode() != ErrorMapping.NoError()) {
                    throw new ConsumerOffsetsException("Error in ConsumerMetadataResponse", metadataResponse.errorCode());
                }

                final Broker offsetManager = metadataResponse.coordinator();
                // if the coordinator is different from the above channel's host (and port) then reconnect
                if (!(offsetManager.host().equals(channel.host())
                        && offsetManager.port() == channel.port())) {
                    channel.disconnect();
                    channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                            BlockingChannel.UseDefaultBufferSize(),
                            BlockingChannel.UseDefaultBufferSize(),
                            READ_TIMEOUT_MS);
                    channel.connect();
                    if (!channel.isConnected()) {
                        logger.error("not connected!");
                    }
                }

                // Make the connected channel's corresponding HostAndPort first
                // in the brokers list to facilitate future connects
                HostAndPort coordinator = new HostAndPort(offsetManager.host(), offsetManager.port());
                int index = brokers.indexOf(coordinator);
                if (index > 0) {
                    Collections.rotate(brokers, brokers.size() - index);
                }

                break;
            } catch (ConsumerOffsetsException e) {
                if (channel.isConnected()) {
                    channel.disconnect();
                }
                if (brokers.indexOf(broker) < brokers.size() - 1) {
                    continue;
                }
                throw e;
                // Client should retry (after backoff)
            } catch (Exception e) {
                if (channel.isConnected()) {
                    channel.disconnect();
                }
                if (brokers.indexOf(broker) < brokers.size() - 1) {
                    continue;
                }
                throw new ConsumerOffsetsException(e);
                // Client should retry query (after backoff)
            }
        }
        return channel;
    }

    public void commitOffsets(final Map<TopicAndPartition, Long> offsets, Handler<AsyncResult<Void>> resultHandler) {
        vertx.executeBlocking(offsetFuture -> {
            try {
                commitOffsetsImpl(offsets);
                offsetFuture.complete();
            } catch (Throwable t) {
                offsetFuture.fail(t);
            }
        }, resultHandler);
    }

    private void commitOffsetsImpl(Map<TopicAndPartition, Long> offsets) throws ConsumerOffsetsException {
        BlockingChannel channel = getConnectionToOffsetManager(groupId);

        final long now = System.currentTimeMillis();
        final Map<TopicAndPartition, OffsetAndMetadata> requestInfo = new LinkedHashMap<>();
        offsets.forEach((topicAndPartition, offset) -> requestInfo.put(topicAndPartition, new OffsetAndMetadata(offset, DEFAULT_COMMIT_METADATA, now)));
        final OffsetCommitRequest commitRequest = new OffsetCommitRequest(
                groupId,
                requestInfo,
                correlationId.getAndIncrement(),
                CLIENT_NAME,
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
            throw e;
        } catch (Exception e) {
            throw new ConsumerOffsetsException(e);
        } finally {
            if (channel != null && channel.isConnected()) {
                channel.disconnect();
            }
        }
    }

    public long fetchOffset(final TopicAndPartition topicAndPartition) throws ConsumerOffsetsException {
        BlockingChannel channel = getConnectionToOffsetManager(groupId);

        final List<TopicAndPartition> partitions = new ArrayList<>();
        partitions.add(topicAndPartition);
        final OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                groupId,
                partitions,
                VERSION_ID,
                correlationId.incrementAndGet(),
                CLIENT_NAME + "_" + topicAndPartition.topic() + "_" + topicAndPartition.partition());
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
            throw e;
        } catch (Throwable t) {
            throw new ConsumerOffsetsException(t);
            // Client should retry the commit
        } finally {
            if (channel != null && channel.isConnected()) {
                channel.disconnect();
            }
        }
    }
}
