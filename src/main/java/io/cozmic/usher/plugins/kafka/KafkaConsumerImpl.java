package io.cozmic.usher.plugins.kafka;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

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
    private static final Long DEFAULT_RETRY_DELAY_MILLIS = 1_000L; // 1 second
    private static final String PUBLISHED_ERRORS_EVENTBUS_ADDRESS = "KafkaConsumer.Errors";
    private final int port;
    private final List<String> seedBrokers;
    private final String groupId;
    // Mutable fields
    private final List<TopicAndPartition> topicAndPartitions = new CopyOnWriteArrayList<>();
    private SimpleConsumer consumer;
    private ConsumerOffsetsStrategy offsetsStrategy;
    private List<String> replicaBrokers = new ArrayList<>();

    private KafkaConsumerImpl() {
        this.seedBrokers = null;
        this.port = 0;
        this.groupId = null;
    }

    public KafkaConsumerImpl(JsonObject config) {
        JsonArray brokers = config.getJsonArray("seed.brokers", new JsonArray().add("localhost"));
        this.seedBrokers = brokers.getList();
        this.port = config.getInteger("port", 9092);
        this.groupId = config.getString("group.id", "0");
        this.replicaBrokers = new ArrayList<>();
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
        logger.debug(builder.toString());
    }

    private List<MessageAndOffset> fetch(TopicAndPartition topicAndPartition) {
        try {
            List<MessageAndOffset> records = new ArrayList<>();

            final String topic = topicAndPartition.topic();
            final int partition = topicAndPartition.partition();

            String leadBroker = findNewLeader("", topicAndPartition);
            String clientName = CLIENT + "_" + topic + "_" + partition;

            offsetsStrategy = ConsumerOffsetsStrategy.createKafkaOffsetsStrategy(leadBroker, port, groupId);
            long readOffset = offsetsStrategy.getOffset(topicAndPartition);

            int numErrors = 0;
            int maxReads = 1; // TODO: change this
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
                        this.close();
                        throw new ConsumerOffsetsException("5 consecutive fetch requests returned an error", code);
                    }
                    this.close();
                    leadBroker = findNewLeader(leadBroker, topicAndPartition);
                    continue;
                }
                numErrors = 0;

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
                    try {
                        Thread.sleep(1_000);
                    } catch (InterruptedException ie) {
                    }
                }
            }
            this.close();
            return records;

        } catch (Exception e) {
            this.close();
            return null;
        }
    }

    private PartitionMetadata findLeader(TopicAndPartition topicAndPartition) {
        final String topic = topicAndPartition.topic();
        final int partition = topicAndPartition.partition();

        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                // TODO: keep consumer
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
        if (returnMetaData != null) {
            replicaBrokers.clear();
            replicaBrokers.addAll(returnMetaData.replicas().stream().map(Broker::host).collect(Collectors.toList()));
        }
        return returnMetaData;
    }

    private String findNewLeader(String oldLeader, TopicAndPartition topicAndPartition) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;

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
                logger.debug("Found metadata for Topic and Partition.");
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
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
    public void commit(TopicAndPartition topicAndPartition, Long offset) throws Exception {
        String leadBroker = findNewLeader("", topicAndPartition);
        offsetsStrategy = ConsumerOffsetsStrategy.createKafkaOffsetsStrategy(leadBroker, port, groupId);
        logger.info("committing offsets");
        offsetsStrategy.commitOffset(topicAndPartition, offset);
    }

    @Override
    public MessageAndOffset poll(TopicAndPartition topicAndPartition) {
        return fetch(topicAndPartition).get(0);
    }
}
