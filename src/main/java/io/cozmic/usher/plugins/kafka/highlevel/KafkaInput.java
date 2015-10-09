package io.cozmic.usher.plugins.kafka.highlevel;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.cozmic.usher.core.InputPlugin;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.cozmic.usher.plugins.kafka.KafkaConsumerConfig;
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

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * KafkaInput
 * Created by Craig Earley on 10/6/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaInput implements InputPlugin {
    private static final Logger logger = LoggerFactory.getLogger(KafkaInput.class.getName());
    private static final String KEY_SEED_BROKERS = "seed.brokers"; // brokers for offset management
    private JsonObject configObj;
    private KafkaConsumerConfig kafkaConsumerConfig;
    private KafkaLogListener kafkaLogListener;

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

        kafkaLogListener = new KafkaLogListener(vertx);
    }

    private class KafkaLogListener {
        private final Vertx vertx;
        private final KafkaOffsets kafkaOffsets;
        private ConsumerConnector connector;
        private Handler<KafkaMessageStream> delegate = null;

        public KafkaLogListener(Vertx vertx) {
            this.vertx = vertx;
            List<String> brokers = Splitter.on(",").splitToList(configObj.getString(KEY_SEED_BROKERS));
            this.kafkaOffsets = new KafkaOffsets(brokers, kafkaConsumerConfig.getGroupId());
        }

        public void commit(final TopicAndPartition topicAndPartition, final Long offset, Handler<AsyncResult<Void>> asyncResultHandler) {
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
                        this::commit);

                messageStream.pause();
                vertx.runOnContext(event -> delegate.handle(messageStream));
            }
            logger.debug("Shutting down Thread: " + Thread.currentThread().getName());
        }

        public void stop(AsyncResultHandler<Void> stopHandler) {
            vertx.executeBlocking(future -> {
                if (connector != null) {
                    connector.shutdown();
                }
                future.complete();
            }, asyncResult -> stopHandler.handle(Future.succeededFuture()));
        }
    }
}
