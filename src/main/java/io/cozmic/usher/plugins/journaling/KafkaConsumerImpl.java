package io.cozmic.usher.plugins.journaling;

import com.google.common.util.concurrent.SettableFuture;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Consumes records from a Kafka cluster.
 * <p>
 * This consumer is (TODO: not yet) thread safe
 * and should generally be shared  among all
 * threads for best performance.
 * <p>
 * Created by Craig Earley on 9/16/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaConsumerImpl implements KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerImpl.class.getName());
    private static final String CLIENT = KafkaConsumerImpl.class.getSimpleName();
    private static final Long DEFAULT_MAX_DELAY_MILLIS = 10_000L; // 10 seconds
    private static final Integer DEFAULT_MAX_RETRIES = Integer.MAX_VALUE; // default number of connection retries
    private static final Long DEFAULT_RETRY_DELAY_MILLIS = 1_000L; // 1 second
    private static final Double DEFAULT_RETRY_DELAY_MULTIPLIER = 2D; // 500ms times 2 after each retry
    private static final String PUBLISHED_ERRORS_EVENTBUS_ADDRESS = "KafkaConsumer.Errors";
    private final int port;
    private final List<String> seedBrokers;
    //private final Long maxDelayMillis;
    //private final Integer maxRetries;
    //private final Long retryDelayMillis;
    //private final Double retryDelayMultiplier;
    private final String groupId;
    private final Vertx vertx;
    // Mutable fields
    private final List<TopicAndPartition> topicAndPartitions = new CopyOnWriteArrayList<>();
    private SimpleConsumer consumer;
    private ConsumerOffsetsStrategy offsetsStrategy;

    private KafkaConsumerImpl() {
        this.seedBrokers = null;
        this.port = 0;
        //this.maxDelayMillis = null;
        //this.maxRetries = null;
        //this.retryDelayMillis = null;
        //this.retryDelayMultiplier = null;
        this.groupId = null;
        this.vertx = null;
    }

    public KafkaConsumerImpl(Vertx vertx, JsonObject config) {
        JsonArray brokers = config.getJsonArray("seed.brokers", new JsonArray());
        this.seedBrokers = brokers.getList();
        this.port = config.getInteger("port", 0);
        //this.maxDelayMillis = config.getLong("max_delay_millis", DEFAULT_MAX_DELAY_MILLIS);
        //this.maxRetries = config.getInteger("max_retries", DEFAULT_MAX_RETRIES);
        //this.retryDelayMillis = config.getLong("retry_delay_millis", DEFAULT_RETRY_DELAY_MILLIS);
        //this.retryDelayMultiplier = config.getDouble("retry_delay_multiplier", DEFAULT_RETRY_DELAY_MULTIPLIER);
        this.groupId = config.getString("group.id", "0");
        this.vertx = vertx;
    }

    private static void debug_logMessages(ByteBufferMessageSet messageSet) throws Exception {
        if (!logger.isDebugEnabled()) {
            return;
        }

        if (messageSet.sizeInBytes() == 0) {
            return;
        }

        StringBuilder builder = new StringBuilder("All uncommitted messages:\n");
        for (MessageAndOffset messageAndOffset : messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            builder.append(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8") + "\n");
        }
        logger.info(builder.toString());
    }

    private void doCommit(Map<TopicAndPartition, Long> offsets, Handler<AsyncResult<Void>> asyncResultHandler) {
        // Retry commit until it succeeds
        // TODO: Back-off strategy
        vertx.setPeriodic(DEFAULT_RETRY_DELAY_MILLIS, timerId -> {
            try {
                Map.Entry<TopicAndPartition, Long> entry = offsets.entrySet().iterator().next();
                // TODO: How to find the right leader when committing multiple topics
                String leadBroker = findNewLeader("", entry.getKey());
                offsetsStrategy = ConsumerOffsetsStrategy.createKafkaOffsetsStrategy(leadBroker, port, groupId);
                offsetsStrategy.commitOffsets(offsets);
                // Success
                vertx.cancelTimer(timerId); // cancel timer
                asyncResultHandler.handle(Future.succeededFuture());
            } catch (Exception e) {
                vertx.eventBus().publish(PUBLISHED_ERRORS_EVENTBUS_ADDRESS, e.getMessage());
                logger.error("Unable to commit, will retry...", e);
            }
        });
    }

    private void doPoll(TopicAndPartition topicAndPartition, Handler<AsyncResult<Map<String, List<MessageAndOffset>>>> asyncResultHandler) {
        // Retry fetch until it returns data
        // TODO: Back-off strategy
        vertx.setPeriodic(DEFAULT_RETRY_DELAY_MILLIS, timerId -> asyncFetch(topicAndPartition, res -> {
            if (res.failed()) {
                vertx.eventBus().publish(PUBLISHED_ERRORS_EVENTBUS_ADDRESS, res.cause().getMessage());
                logger.error("Unable to fetch data, will retry...", res.cause());
                return;
            }
            Map<String, List<MessageAndOffset>> map = res.result();
            if (map == null || map.isEmpty()) {
                logger.info("No data found, will retry...", res.cause());
                return;
            }
            // Success
            vertx.cancelTimer(timerId); // cancel timer
            asyncResultHandler.handle(Future.succeededFuture(map));
        }));
    }

    private void asyncFetch(TopicAndPartition topicAndPartition, Handler<AsyncResult<Map<String, List<MessageAndOffset>>>> asyncResultHandler) {
        vertx.executeBlocking(future -> {
            try {
                Map<String, List<MessageAndOffset>> topicMap = new HashMap<>();
                List<MessageAndOffset> records = new ArrayList<>();

                final String topic = topicAndPartition.topic();
                final int partition = topicAndPartition.partition();

                String leadBroker = findNewLeader("", topicAndPartition);
                String clientName = CLIENT + "_" + topic + "_" + partition;

                offsetsStrategy = ConsumerOffsetsStrategy.createKafkaOffsetsStrategy(leadBroker, port, groupId);
                long readOffset = offsetsStrategy.getOffset(topicAndPartition);

                int numErrors = 0;
                boolean fetchComplete = false;
                while (!fetchComplete) {
                    if (consumer == null) {
                        consumer = new SimpleConsumer(leadBroker, port, 100_000, 64 * 1024, clientName);
                    }
                    FetchRequest req = new FetchRequestBuilder()
                            .clientId(clientName)
                            .addFetch(topic, partition, readOffset, 100_000) // Note: this fetchSize of 100_000 might need to be increased if large batches are written to Kafka
                            .build();
                    FetchResponse fetchResponse = consumer.fetch(req);
                    debug_logMessages(fetchResponse.messageSet(topic, partition));

                    if (fetchResponse.hasError()) {
                        numErrors++;
                        short code = fetchResponse.errorCode(topic, partition);
                        logger.info("", new ConsumerOffsetsException("Error fetching data from the broker:" + leadBroker, code));
                        if (numErrors > 5) {
                            this.close();
                            throw new ConsumerOffsetsException("5 consecutive fetch requests returned an error", code);
                        }
                        this.close();
                        leadBroker = findNewLeader(leadBroker, topicAndPartition);
                        continue;
                    }

                    fetchComplete = true;

                    long numRead = 0;
                    for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                        long currentOffset = messageAndOffset.offset();
                        if (currentOffset < readOffset) {
                            logger.warn("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                            continue;
                        }
                        readOffset = messageAndOffset.nextOffset();
                        records.add(messageAndOffset);
                        numRead++;
                    }

                    if (numRead == 0) {
                        future.complete(topicMap); // return empty result
                        return;
                    }
                }
                topicMap.put(topic, records);
                future.complete(topicMap); // return data
            } catch (Exception e) {
                this.close();
                future.fail(e); // return error
            }
        }, res -> {
            if (res.failed()) {
                asyncResultHandler.handle(Future.failedFuture(res.cause()));
                return;
            }
            Map<String, List<MessageAndOffset>> map = (Map<String, List<MessageAndOffset>>) res.result();
            asyncResultHandler.handle(Future.succeededFuture(map));
        });
    }

    private PartitionMetadata findLeader(TopicAndPartition topicAndPartition) {
        final String topic = topicAndPartition.topic();
        final int partition = topicAndPartition.partition();

        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, 100_000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        return returnMetaData;
    }

    private String findNewLeader(String oldLeader, TopicAndPartition topicAndPartition) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep;

            PartitionMetadata metadata = findLeader(topicAndPartition);
            if (metadata == null) {
                logger.info("Can't find metadata for Topic and Partition... will try again...");
                goToSleep = true;
            } else if (metadata.leader() == null) {
                logger.info("Can't find Leader for Topic and Partition... will try again...");
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                logger.info("Found metadata for Topic and Partition.");
                return metadata.leader().host();
            }
            if (goToSleep) {
                // TODO: This part no longer works - though it did before
//                final boolean[] sleep = {true};
//                vertx.setTimer(5_000, timerId -> sleep[0] = false);
//                while (sleep[0]) {
//                }
//                logger.info("Resuming after 5 second delay");
                logger.info("No leader found.");
                throw new Exception("No leader found");
            }
        }
        String msg = "Unable to find new leader after Broker failure.";
        logger.error(msg);
        throw new Exception(msg);
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }

    @Override
    public java.util.concurrent.Future<AsyncResult<Void>> commit(Map<TopicAndPartition, Long> offsets) {
        final SettableFuture<AsyncResult<Void>> future = SettableFuture.create();

        doCommit(offsets, result -> {
            if (result.failed()) {
                future.setException(result.cause());
                return;
            }
            future.set(result);
        });
        return future;
    }

    @Override
    public java.util.concurrent.Future<AsyncResult<Void>> commit(TopicAndPartition topicAndPartition, Long offset) {
        final SettableFuture<AsyncResult<Void>> future = SettableFuture.create();
        final Map<TopicAndPartition, Long> offsets = new HashMap<>();
        offsets.put(topicAndPartition, offset);

        doCommit(offsets, result -> {
            if (result.failed()) {
                future.setException(result.cause());
                return;
            }
            future.set(result);
        });
        return future;
    }

    @Override
    public void poll(Handler<AsyncResult<Map<String, List<MessageAndOffset>>>> asyncResultHandler) {
        logger.error("Not implemented");
    }

    @Override
    public void poll(TopicAndPartition topicAndPartition, Handler<AsyncResult<Map<String, List<MessageAndOffset>>>> asyncResultHandler) {
        doPoll(topicAndPartition, asyncResultHandler);
    }

    @Override
    public java.util.concurrent.Future<AsyncResult<Map<String, List<MessageAndOffset>>>> poll() {
        final SettableFuture<AsyncResult<Map<String, List<MessageAndOffset>>>> future = SettableFuture.create();
        future.setException(new Exception("Not implemented"));
        return future;
    }

    @Override
    public java.util.concurrent.Future<AsyncResult<Map<String, List<MessageAndOffset>>>> poll(TopicAndPartition topicAndPartition) {
        final SettableFuture<AsyncResult<Map<String, List<MessageAndOffset>>>> future = SettableFuture.create();
        doPoll(topicAndPartition, result -> {
            if (result.failed()) {
                future.setException(result.cause());
                return;
            }
            future.set(result);
        });
        return future;
    }

    @Override
    public void subscribe(TopicAndPartition... partitions) {
        for (TopicAndPartition topicAndPartition : partitions) {
            if (!topicAndPartitions.contains(topicAndPartition)) {
                topicAndPartitions.add(topicAndPartition);
            }
        }
    }

    @Override
    public void unsubscribe(TopicAndPartition... partitions) {
        for (TopicAndPartition topicAndPartition : partitions) {
            if (topicAndPartitions.contains(topicAndPartition)) {
                topicAndPartitions.remove(topicAndPartition);
            }
        }
    }
}
