package io.cozmic.usher.plugins.journaling;

import io.cozmic.usher.core.InputPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Connects to a Kafka broker and consumes messages
 * from the specified topic and partition.
 * <p>
 * Created by Craig Earley on 8/26/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaInput implements InputPlugin {
    private static final String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
    private static final String GROUP_ID = "group.id";
    private static final String TOPIC = "topic";
    private static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    private static final String ZOOKEEPER_SESSION_TIMEOUT_MS = "zookeeper.session.timeout.ms";
    private static final String ZOOKEEPER_SYNC_TIME_MS = "zookeeper.sync.time.ms";
    private static final Logger logger = LoggerFactory.getLogger(KafkaInput.class.getName());
    private ConsumerConfig consumerConfig;
    private JsonObject configObj;
    private Vertx vertx;

    @Override
    public void run(AsyncResultHandler<Void> startupHandler, Handler<DuplexStream<Buffer, Buffer>> duplexStreamHandler) {

        getKafkaConsumerStream(asyncResult -> {
            if (asyncResult.failed()) {
                startupHandler.handle(Future.failedFuture(asyncResult.cause()));
                return;
            }
            startupHandler.handle(Future.succeededFuture());

            final KafkaConsumer kafkaconsumer = asyncResult.result();

            duplexStreamHandler.handle(new DuplexStream<>(kafkaconsumer, kafkaconsumer, pack -> {
                final Message message = pack.getMessage();
                // Data should enter the pipeline here.
                message.setLocalAddress(EmptyAddress.emptyAddress());
                message.setRemoteAddress(EmptyAddress.emptyAddress());
            }, v -> kafkaconsumer.stop()));
            /*
             * TODO: The number of threads revolves around the number of partitions
             * in the topic and there are some very specific rules. (Read the docs)
             */
            kafkaconsumer.run(1);
        });
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {
        this.configObj = configObj;
        this.consumerConfig = createConsumerConfig();
        this.vertx = vertx;
    }

    private void getKafkaConsumerStream(AsyncResultHandler<KafkaConsumer> asyncResultHandler) {
        vertx.executeBlocking(event -> {
            event.complete(new KafkaConsumer());
        }, new Handler<AsyncResult<KafkaConsumer>>() {
            @Override
            public void handle(AsyncResult<KafkaConsumer> result) {
                asyncResultHandler.handle(result);
            }
        });
    }

    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put(ZOOKEEPER_CONNECT, configObj.getString(ZOOKEEPER_CONNECT, ""));
        props.put(GROUP_ID, configObj.getString(GROUP_ID, ""));
        props.put(ZOOKEEPER_SESSION_TIMEOUT_MS, configObj.getInteger(ZOOKEEPER_SESSION_TIMEOUT_MS, 400).toString());
        props.put(ZOOKEEPER_SYNC_TIME_MS, configObj.getInteger(ZOOKEEPER_SYNC_TIME_MS, 200).toString());
        props.put("auto.commit.interval.ms", configObj.getInteger(AUTO_COMMIT_INTERVAL_MS, 1000).toString());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "smallest");
        return new ConsumerConfig(props);
    }

    private static class EmptyAddress implements SocketAddress {
        public static SocketAddress emptyAddress() {
            return new EmptyAddress();
        }

        @Override
        public String host() {
            return "";
        }

        @Override
        public int port() {
            return 0;
        }
    }

    private class KafkaConsumer implements ReadStream<Buffer>, WriteStream<Buffer> {
        private final ConsumerConnector consumer;
        private final String topic;
        private Handler<Buffer> handler;
        private ExecutorService executor;

        public KafkaConsumer() {
            consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
            topic = configObj.getString(TOPIC);
        }

        public void stop() {
            if (consumer != null) {
                consumer.shutdown();
            }
            if (executor == null) {
                return;
            }
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    logger.error("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted during shutdown, exiting uncleanly");
            }
        }

        private void run(int numberOfThreads) {

            Map<String, Integer> topicCountMap = new HashMap<>();
            topicCountMap.put(topic, new Integer(numberOfThreads));
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

            executor = Executors.newFixedThreadPool(numberOfThreads);

            int threadNumber = 0;
            for (final KafkaStream stream : streams) {
                final int finalThreadNumber = threadNumber;
                executor.submit(() -> {
                    // Note: iterator blocks until messages available.
                    ConsumerIterator<byte[], byte[]> it = stream.iterator();
                    while (it.hasNext()) {
                        byte[] data = it.next().message();
                        logger.debug("Thread " + finalThreadNumber + ": " + new String(data));
                        handler.handle(Buffer.buffer(data));
                    }
                    logger.debug("Shutting down Thread: " + finalThreadNumber);
                });
                threadNumber++;
            }
        }

        @Override
        public KafkaConsumer exceptionHandler(Handler<Throwable> handler) {
            return this;
        }

        @Override
        public WriteStream<Buffer> write(Buffer data) {
            handler.handle(data);
            return this;
        }

        @Override
        public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
            return this;
        }

        @Override
        public boolean writeQueueFull() {
            return false;
        }

        @Override
        public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
            return this;
        }

        @Override
        public ReadStream<Buffer> handler(Handler<Buffer> handler) {
            this.handler = handler;
            return null;
        }

        @Override
        public ReadStream<Buffer> pause() {
            return this;
        }

        @Override
        public ReadStream<Buffer> resume() {
            return this;
        }

        @Override
        public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
            return this;
        }
    }
}
