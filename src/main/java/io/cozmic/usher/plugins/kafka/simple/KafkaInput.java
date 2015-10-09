package io.cozmic.usher.plugins.kafka.simple;

import io.cozmic.usher.core.InputPlugin;
import io.cozmic.usher.plugins.kafka.KafkaMessageStream;
import io.cozmic.usher.streams.DuplexStream;
import io.cozmic.usher.streams.NullClosableWriteStream;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connects to a Kafka broker and consumes messages
 * from the specified topic and partition.
 * <p>
 * Created by Craig Earley on 8/26/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaInput implements InputPlugin {
    private static final String TOPIC = "topic";
    private static final String PARTITIONS = "partitions";
    private static final Logger logger = LoggerFactory.getLogger(KafkaInput.class.getName());
    private JsonObject configObj;
    private Vertx vertx;
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

        String topic = configObj.getString(TOPIC);
        Integer partitions = configObj.getInteger(PARTITIONS, 1);

        kafkaLogListener.listen(topic, partitions, asyncResult -> {
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
    public void init(JsonObject configObj, Vertx vertx) {
        this.configObj = configObj;
        this.vertx = vertx;
        kafkaLogListener = new KafkaLogListener(configObj, vertx);
    }

    private class KafkaLogListener {
        private static final String SEED_BROKERS = "seed.brokers";
        private static final String REPLY_TOPIC = "reply.topic";
        private static final String KEY_SERIALIZER = "key.serializer";
        private static final String VALUE_SERIALIZER = "value.serializer";
        private static final String CONSUMER_ADDRESS = "io.cozmic.usher.plugins.kafka.KafkaLogListener";
        private final AtomicBoolean isStopped = new AtomicBoolean();
        private boolean responseHandlerShouldCommit; // false (default) - KafkaMessageStream should commit
        private ConcurrentMap<TopicAndPartition, KafkaConsumer> consumerMap;
        private Handler<KafkaMessageStream> delegate = null;
        private JsonObject configObj;
        private Vertx vertx;

        public KafkaLogListener(JsonObject configObj, Vertx vertx) {
            this.configObj = configObj;
            this.vertx = vertx;
        }

        public void asyncCommit(final TopicAndPartition topicAndPartition, Long offset, Handler<AsyncResult<Void>> asyncResultHandler) {
            vertx.executeBlocking(future -> {
                // Get the consumer for this topicAndPartition
                KafkaConsumer consumer = consumerMap.get(topicAndPartition);

                try {
                    // Commit offset for this topic and partition
                    consumer.commit(topicAndPartition, offset);

                    future.complete(); // success
                } catch (Exception e) {
                    future.fail(e); // failure
                }
            }, false, asyncResult -> {
                if (asyncResult.failed()) {
                    asyncResultHandler.handle(Future.failedFuture(asyncResult.cause()));
                    return;
                }
                vertx.eventBus().publish(CONSUMER_ADDRESS, topicAndPartition);
                asyncResultHandler.handle(Future.succeededFuture());
            });
        }

        public KafkaLogListener listen(String topic, int partitions, Handler<AsyncResult<KafkaLogListener>> listenHandler) {
            // Register message codec to send TopicAndPartition objects across the event bus
            MessageCodec codec = new TopicAndPartitionCodec();
            vertx.eventBus().registerDefaultCodec(TopicAndPartition.class, codec);

            run(); // subscribe to consumer events

            consumerMap = new ConcurrentHashMap<>(partitions);

            // Fire initial consumer events to bootstrap the consumer threads
            for (int i = 0; i < partitions; i++) {
                TopicAndPartition topicAndPartition = new TopicAndPartition(topic, i);
                consumerMap.put(topicAndPartition, new KafkaConsumerImpl(configObj));
                vertx.eventBus().publish(CONSUMER_ADDRESS, topicAndPartition);
            }

            listenHandler.handle(Future.succeededFuture());
            return this;
        }

        public KafkaLogListener logHandler(Handler<KafkaMessageStream> delegate, boolean responseHandlerShouldCommit) {
            this.delegate = delegate;
            this.responseHandlerShouldCommit = responseHandlerShouldCommit;
            return this;
        }

        public KafkaLogListener logHandler(Handler<KafkaMessageStream> delegate) {
            return logHandler(delegate, false);
        }

        private Properties getKafkaProducerProperties() {
            Properties properties = new Properties();

            // Defaults
            properties.put("bootstrap.servers", "localhost:9092");
            properties.put(KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
            properties.put(VALUE_SERIALIZER, "org.apache.kafka.common.serialization.ByteArraySerializer");

            if (configObj.containsKey(SEED_BROKERS)) {
                int port = configObj.getInteger("port", 9092);
                List<String> bootstrapServers = new ArrayList<>();
                configObj.getJsonArray(SEED_BROKERS).forEach(s -> bootstrapServers.add(s + ":" + port));
                properties.put("bootstrap.servers", String.join(",", bootstrapServers));
            }
            if (configObj.containsKey(KEY_SERIALIZER)) {
                properties.put(KEY_SERIALIZER, configObj.getString(KEY_SERIALIZER));
            }
            if (configObj.containsKey(VALUE_SERIALIZER)) {
                properties.put(VALUE_SERIALIZER, configObj.getString(VALUE_SERIALIZER));
            }

            return properties;
        }

        private void run() {
            vertx.eventBus().consumer(CONSUMER_ADDRESS, msg -> {
                if (isStopped.get()) {
                    return;
                }
                // Every 'event' should be a TopicAndPartition instance
                if (!(msg.body() instanceof TopicAndPartition)) {
                    return;
                }
                final TopicAndPartition topicAndPartition = (TopicAndPartition) msg.body();

                // executeBlocking is called with 'false' as the argument to ordered
                // so that threads are executed in parallel on the worker pool.
                vertx.executeBlocking(future -> {
                    if (delegate == null) {
                        future.fail("KafkaLogListener.logHandler not setup"); // failure
                        return;
                    }

                    // Get the consumer for this topicAndPartition
                    KafkaConsumer consumer = consumerMap.get(topicAndPartition);

                    // Poll for message data at earliest uncommitted offset
                    MessageAndOffset messageAndOffset = consumer.poll(topicAndPartition); // todo: timeout?

                    if (messageAndOffset == null) {
                        future.fail("Data or timeout error"); // failure
                        return;
                    }

                    // Get message bytes
                    ByteBuffer payload = messageAndOffset.message().payload();
                    byte[] bytes = new byte[payload.limit()];
                    payload.get(bytes);

                    KafkaMessageStream messageStream = new KafkaMessageStream(
                            Buffer.buffer(bytes),
                            topicAndPartition,
                            messageAndOffset,
                            this::asyncCommit);

                    if (responseHandlerShouldCommit) {
                        // Setup pipeline stream handler to handle commit internally
                        messageStream.responseHandler(data -> {
                            if (future.failed()) {
                                // Future may have been set to failed after timeout
                                return;
                            }
                            try {
                                // Commit offset for this topic and partition
                                consumer.commit(topicAndPartition, messageAndOffset.offset());

                                future.complete(true); // success (responseHandlerDidCommit = true)
                            } catch (Exception e) {
                                future.fail(e); // failure (Nothing committed or sent to the reply topic)
                            }
                        });
                    }

                    messageStream.pause();
                    delegate.handle(messageStream);

                    if (!responseHandlerShouldCommit) {
                        future.complete(false); // success (responseHandlerDidCommit = false)
                    } else {
                        // TODO: Configurable timeout?
                        vertx.setTimer(10_000, timerId -> future.fail("MessageStream response timeout"));
                    }

                }, false, asyncResult -> {
                    if (asyncResult.failed()) {
                        logger.error(asyncResult.cause());
                        // TODO: Should certain failures stop new events for topic+partition?
                        //return;
                    }
                    boolean responseHandlerDidCommit = (boolean) asyncResult.result();
                    if (responseHandlerDidCommit) {
                        vertx.eventBus().publish(CONSUMER_ADDRESS, topicAndPartition);
                    }
                });
            });
        }

        public void stop(AsyncResultHandler<Void> stopHandler) {
            isStopped.set(true);
            stopHandler.handle(Future.succeededFuture());
        }
    }
}
