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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * KafkaConsumerImpl
 * Created by Craig Earley on 9/16/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaConsumerImpl implements KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerImpl.class.getName());
    private static final String CLIENT = KafkaConsumerImpl.class.getSimpleName();
    private static final int MAX_READS = 1; // First record since OffsetRequest.EarliestTime()
    private final int port;
    private final List<String> seedBrokers;
    private final List<TopicAndPartition> topicAndPartitions = new CopyOnWriteArrayList<>();
    private final String groupId;
    private final Vertx vertx;
    private SimpleConsumer consumer;
    private ConsumerOffsetsStrategy offsetsStrategy;

    private KafkaConsumerImpl() {
        this.seedBrokers = null;
        this.port = 0;
        this.groupId = null;
        this.vertx = null;
    }

    public KafkaConsumerImpl(Vertx vertx, JsonObject config) {
        JsonArray brokers = config.getJsonArray("seed.brokers", new JsonArray());
        this.seedBrokers = brokers.getList();
        this.port = config.getInteger("port", 0);
        this.groupId = config.getString("group.id", "0");
        this.vertx = vertx;
    }

    private static void debug_logMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        for (MessageAndOffset messageAndOffset : messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            logger.info(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
        }
    }

    private Map<String, List<MessageAndOffset>> fetch(TopicAndPartition topicAndPartition, long maxReads) throws Exception {
        Map<String, List<MessageAndOffset>> topicMap = new HashMap<>();
        List<MessageAndOffset> records = new ArrayList<>();

        final String topic = topicAndPartition.topic();
        final int partition = topicAndPartition.partition();

        String leadBroker = findNewLeader("", topicAndPartition);
        String clientName = CLIENT + "_" + topic + "_" + partition;

        consumer = new SimpleConsumer(leadBroker, port, 100_000, 64 * 1024, clientName);
        offsetsStrategy = new KafkaOffsets(leadBroker, port, groupId);

        long readOffset = offsetsStrategy.getOffset(topicAndPartition);

        int numErrors = 0;
        while (maxReads > 0) {
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
                    throw new ConsumerOffsetsException("5 consecutive fetch requests returned an error", code);
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, topicAndPartition);
                continue;
            }

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
                maxReads--;
            }

            if (numRead == 0) {
                logger.info("No data found - Sleeping for 5 seconds...");
                final boolean[] sleep = {true};
                vertx.setTimer(5_000, timerId -> sleep[0] = false);
                while (sleep[0]) {
                }
                logger.info("... resuming fetch after 5 second delay");
            }
        }
        if (consumer != null) consumer.close();
        topicMap.put(topic, records);
        return topicMap;
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
                final boolean[] sleep = {true};
                vertx.setTimer(5_000, timerId -> sleep[0] = false);
                while (sleep[0]) {
                }
                logger.info("Resuming after 5 second delay");
            }
        }
        String msg = "Unable to find new leader after Broker failure.";
        logger.error(msg);
        throw new Exception(msg);
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

    private void doCommit(Map<TopicAndPartition, Long> offsets, Handler<AsyncResult<Void>> asyncResultHandler) {
        // todo: retry with back-off
        vertx.executeBlocking(future -> {
            try {
                Map.Entry<TopicAndPartition, Long> entry = offsets.entrySet().iterator().next();
                String leadBroker = findNewLeader("", entry.getKey());
                offsetsStrategy = new KafkaOffsets(leadBroker, port, groupId);
                offsetsStrategy.commitOffsets(offsets);
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, res -> {
            if (res.failed()) {
                asyncResultHandler.handle(Future.failedFuture(res.cause()));
                return;
            }
            asyncResultHandler.handle(Future.succeededFuture());
        });
    }

    private void doPoll(Handler<AsyncResult<Map<String, List<MessageAndOffset>>>> asyncResultHandler, TopicAndPartition... topicAndPartitions) {
        // todo: retry with back-off
        vertx.executeBlocking(future -> {
            try {
                Map<String, List<MessageAndOffset>> map = new HashMap<>();
                for (TopicAndPartition topicAndPartition : topicAndPartitions) {
                    map.putAll(fetch(topicAndPartition, MAX_READS));
                }
                future.complete(map);
            } catch (Exception e) {
                future.fail(e);
            }
        }, res -> {
            if (res.failed()) {
                asyncResultHandler.handle(Future.failedFuture(res.cause()));
                return;
            }
            Map<String, List<MessageAndOffset>> result = (Map<String, List<MessageAndOffset>>) res.result();
            asyncResultHandler.handle(Future.succeededFuture(result));
        });
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
        doPoll(asyncResultHandler, topicAndPartitions.toArray(new TopicAndPartition[topicAndPartitions.size()]));

    }

    @Override
    public void poll(TopicAndPartition topicAndPartition, Handler<AsyncResult<Map<String, List<MessageAndOffset>>>> asyncResultHandler) {
        doPoll(asyncResultHandler, topicAndPartition);
    }

    @Override
    public java.util.concurrent.Future<AsyncResult<Map<String, List<MessageAndOffset>>>> poll() {
        final SettableFuture<AsyncResult<Map<String, List<MessageAndOffset>>>> future = SettableFuture.create();
        doPoll(result -> {
            if (result.failed()) {
                future.setException(result.cause());
                return;
            }
            future.set(result);
        }, topicAndPartitions.toArray(new TopicAndPartition[topicAndPartitions.size()]));
        return future;
    }

    @Override
    public java.util.concurrent.Future<AsyncResult<Map<String, List<MessageAndOffset>>>> poll(TopicAndPartition topicAndPartition) {
        final SettableFuture<AsyncResult<Map<String, List<MessageAndOffset>>>> future = SettableFuture.create();
        doPoll(result -> {
            if (result.failed()) {
                future.setException(result.cause());
                return;
            }
            future.set(result);
        }, topicAndPartition);
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
