package io.cozmic.usher.plugins.kafka.highlevel;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.cozmic.usher.core.InputPlugin;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.cozmic.usher.plugins.kafka.KafkaMessageStream;
import io.cozmic.usher.plugins.kafka.KafkaOffsets;
import io.cozmic.usher.streams.DuplexStream;
import io.cozmic.usher.streams.NullClosableWriteStream;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * KafkaInput
 * Created by Craig Earley on 10/6/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaInput implements InputPlugin {
    private final static ThreadFactory FACTORY = new ThreadFactoryBuilder().setNameFormat("kafka-consumer-thread-%d").build();
    private static final Logger logger = LoggerFactory.getLogger(KafkaInput.class.getName());
    private static final String KEY_REPLY_TOPIC = "reply.topic"; // topic to send reply after stream.commit
    private static final String KEY_SEED_BROKERS = "seed.brokers"; // brokers for offset management
    private KafkaLogListener kafkaLogListener;
    private JsonObject configObj;
    private KafkaConsumerConfig kafkaConsumerConfig;
    private KafkaProducerConfig kafkaProducerConfig;

    @Override
    public void run(AsyncResultHandler<Void> startupHandler, Handler<DuplexStream<Buffer, Buffer>> duplexStreamHandler) {

        kafkaLogListener.logHandler(stream -> {
            final DuplexStream<Buffer, Buffer> duplexStream = new DuplexStream<>(stream, NullClosableWriteStream.getInstance());

            duplexStream
                    .closeHandler(v -> stream.close())
                    .writeCompleteHandler(pack -> {
                        stream.commit(pack);
                    });

            duplexStreamHandler.handle(duplexStream);
        });

        kafkaLogListener.listen(asyncResult -> {
            if (asyncResult.failed()) {
                startupHandler.handle(Future.failedFuture(asyncResult.cause()));
                return;
            }
            logger.info("KafkaLogListener started: " + configObj);
            startupHandler.handle(Future.succeededFuture());
        });
    }

    @Override
    public void stop(AsyncResultHandler<Void> stopHandler) {
        logger.info("Stopping Kafka Input");
        kafkaLogListener.stop(stopHandler);
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) throws UsherInitializationFailedException {
        this.configObj = configObj;

        Preconditions.checkState(configObj.containsKey(KEY_SEED_BROKERS), "No configuration for key " + KEY_SEED_BROKERS);

        kafkaConsumerConfig = KafkaConsumerConfig.create(
                configObj.getString(KafkaConsumerConfig.KEY_GROUP_ID),
                configObj.getString(KafkaConsumerConfig.KEY_KAFKA_TOPIC),
                configObj.getString(KafkaConsumerConfig.KEY_ZOOKEEPER),
                configObj.getString(KafkaConsumerConfig.KEY_ZOOKEEPER_TIMEOUT_MS, "100000"),
                configObj.getInteger(KafkaConsumerConfig.KEY_PARTITIONS));

        kafkaProducerConfig = KafkaProducerConfig.create(
                configObj.getString(KEY_REPLY_TOPIC, ""),
                configObj.getString(KEY_SEED_BROKERS),
                configObj.getString(KafkaProducerConfig.KEY_KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer"),
                configObj.getString(KafkaProducerConfig.KEY_VALUE_SERIALIZER, "org.apache.kafka.common.serialization.ByteArraySerializer")
        );

        kafkaLogListener = new KafkaLogListener(vertx);
    }

    private class KafkaLogListener {
        private final com.cyberphysical.streamprocessing.KafkaProducer<String, byte[]> producer;
        private final Vertx vertx;
        private ConsumerConnector connector;
        private ExecutorService executor = Executors.newSingleThreadExecutor(FACTORY);
        private Handler<KafkaMessageStream> delegate = null;
        private KafkaOffsets kafkaOffsets;

        public KafkaLogListener(Vertx vertx) {
            this.vertx = vertx;
            this.producer = new com.cyberphysical.streamprocessing.KafkaProducer<>(vertx, kafkaProducerConfig.getProperties());
            List<String> brokers = Splitter.on(",").splitToList(configObj.getString(KEY_SEED_BROKERS));
            this.kafkaOffsets = new KafkaOffsets(brokers, kafkaConsumerConfig.getGroupId());
        }

        public void commit(final TopicAndPartition topicAndPartition, Long offset, Handler<AsyncResult<Void>> asyncResultHandler) {
            vertx.executeBlocking(future -> {
                try {
                    // Commit offset for this topic and partition
                    kafkaOffsets.commitOffset(topicAndPartition, offset);

                    logger.debug(String.format("committed %s at offset: %d", topicAndPartition, offset));

                    future.complete();
                } catch (Exception e) {
                    future.fail(e);
                }
            }, false, asyncResult -> {
                if (asyncResult.failed()) {
                    asyncResultHandler.handle(Future.failedFuture(asyncResult.cause()));
                    return;
                }
                asyncResultHandler.handle(Future.succeededFuture());
            });
        }

        public void sendToReplyTopic(byte[] value, Handler<AsyncResult<Void>> asyncResultHandler) {
            String replyTopic = kafkaProducerConfig.getKafkaTopic();
            if (Strings.isNullOrEmpty(replyTopic)) {
                asyncResultHandler.handle(Future.succeededFuture()); // reply topic is optional
                return;
            }
            // Send message to reply topic.
            producer.send(replyTopic, value, event -> {
                if (event.failed()) {
                    logger.error("Error sending message to reply topic: ", event.cause());
                    asyncResultHandler.handle(Future.failedFuture(event.cause()));
                    return;
                }
                RecordMetadata metadata = event.result();
                logger.debug(String.format("Sent message: offset: %d, topic: %s, partition: %d",
                        metadata.offset(), metadata.topic(), metadata.partition()));
                asyncResultHandler.handle(Future.succeededFuture());
            });
        }

        public KafkaLogListener listen(Handler<AsyncResult<KafkaLogListener>> listenHandler) {
            vertx.executeBlocking(future -> {
                try {
                    run(kafkaConsumerConfig.getPartitions());
                    future.complete();
                } catch (Exception e) {
                    future.fail(e);
                }
            }, asyncResult -> {
                if (asyncResult.failed()) {
                    listenHandler.handle(Future.failedFuture(asyncResult.cause()));
                    return;
                }
                listenHandler.handle(Future.succeededFuture());
            });
            return this;
        }

        public KafkaLogListener logHandler(Handler<KafkaMessageStream> delegate) {
            this.delegate = delegate;
            return this;
        }

        protected ConsumerConfig createConsumerConfig() {
            final Properties properties = kafkaConsumerConfig.getProperties();

            properties.setProperty("auto.commit.enable", Boolean.FALSE.toString());
            properties.setProperty("auto.offset.reset", "smallest");
            properties.setProperty("queued.max.message.chunks", "1000");
            properties.setProperty("offsets.storage", "kafka");

            return new ConsumerConfig(properties);
        }

        private void run(int numberOfThreads) {
            connector = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
            final String topic = kafkaConsumerConfig.getKafkaTopic();
            final Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = connector.createMessageStreams(ImmutableMap.of(topic, numberOfThreads));
            final List<KafkaStream<byte[], byte[]>> topicStreams = messageStreams.get(topic);

            topicStreams.forEach(stream -> vertx.executeBlocking(future -> read(topic, stream), false, null));
        }

        private void read(final String topic, final KafkaStream<byte[], byte[]> stream) {
            logger.debug("[Worker] Starting in " + Thread.currentThread().getName());

            while (stream.iterator().hasNext()) {
                final MessageAndMetadata<byte[], byte[]> msg = stream.iterator().next();
                final long offset = msg.offset();
                final int partition = msg.partition();

                // Get message bytes
                final byte[] bytes = msg.message();

                logger.debug("Consumed " + Buffer.buffer(bytes).toString() + " at offset " + offset + " on thread: " + Thread.currentThread().getName());

                final KafkaMessageStream messageStream = new KafkaMessageStream(
                        Buffer.buffer(bytes),
                        new TopicAndPartition(topic, partition),
                        new MessageAndOffset(new kafka.message.Message(bytes), offset),
                        this::commit,
                        this::sendToReplyTopic);

                messageStream.pause();
                vertx.runOnContext(event -> delegate.handle(messageStream));
            }
            logger.debug("Shutting down Thread: " + Thread.currentThread().getName());
        }

        public void stop(AsyncResultHandler<Void> stopHandler) {
            if (executor != null) {
                executor.shutdown();
            }
            if (connector != null) {
                connector.shutdown();
            }
            stopHandler.handle(Future.succeededFuture());
        }
    }
}
