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
import io.cozmic.usher.streams.WriteCompleteFuture;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import rx.Observable;

import java.util.ArrayList;
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

        kafkaLogListener.logHandler(stream -> doCreateStream(duplexStreamHandler, stream));

        kafkaLogListener.listen(asyncResult -> {
            if (asyncResult.failed()) {
                startupHandler.handle(Future.failedFuture(asyncResult.cause()));
                return;
            }
            logger.info("KafkaLogListener started: " + configObj);
            startupHandler.handle(Future.succeededFuture());
        });
    }

    private void doCreateStream(Handler<DuplexStream<Buffer, Buffer>> duplexStreamHandler, KafkaMessageStream stream) {
        final DuplexStream<Buffer, Buffer> duplexStream = new DuplexStream<>(stream, NullClosableWriteStream.getInstance());

        duplexStream
                .closeHandler(v -> {
                    logger.info("KafkaInput - Close requested. Kafka streams don't actually close. Instead this is " +
                            "interpreted to mean that there was a problem processing the last message. For now we're just going to " +
                            "commit and abandon that message. However, we plan to add a dead-letter-queue type feature here at " +
                            "some point. Typically this will only occur when there are decoding errors. It could also happen if an " +
                            "error strategy is setup that allows the error to bubble back. In most cases though we intend to " +
                            "explicitly setup error strategies that will ensure processing.");

                    //TODO: Add dead letter queue feature
                    stream.commit(WriteCompleteFuture.future(null));
                    logger.info("Restarting channel for stream");
                    doCreateStream(duplexStreamHandler, stream);
                })
                .writeCompleteHandler(stream::commit);

        duplexStreamHandler.handle(duplexStream);
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
                configObj.getString(KafkaConsumerConfig.KEY_ZOOKEEPER_TIMEOUT_MS, "120000"),
                configObj.getInteger(KafkaConsumerConfig.KEY_PARTITIONS, 1));

        kafkaLogListener = new KafkaLogListener(vertx, configObj.getInteger("numberOfThreads", 10));
    }

    private class KafkaLogListener {
        private final Vertx vertx;


        private ConsumerConnector connector;
        private Handler<KafkaMessageStream> delegate = null;
        private ArrayList<KafkaMessageStream> kafkaMessageStreams = new ArrayList<>();
        private int numberOfThreads;
        private KafkaOffsets kafkaOffsets;

        public KafkaLogListener(Vertx vertx, int numberOfThreads) {
            this.vertx = vertx;
            this.numberOfThreads = numberOfThreads;
            List<String> brokers = Splitter.on(",").splitToList(configObj.getString(KEY_SEED_BROKERS));
            kafkaOffsets = new KafkaOffsets(vertx, brokers, kafkaConsumerConfig.getGroupId());
        }


        public KafkaLogListener listen(Handler<AsyncResult<KafkaLogListener>> listenHandler) {
            final Context context = vertx.getOrCreateContext();

            vertx.executeBlocking(future -> {
                try {
                    connector = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());

                    final String topic = kafkaConsumerConfig.getKafkaTopic();
                    final Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = connector.createMessageStreams(ImmutableMap.of(topic, numberOfThreads));
                    final List<KafkaStream<byte[], byte[]>> topicStreams = messageStreams.get(topic);


                    topicStreams.forEach(stream -> {
                        final KafkaMessageStream messageStream = new KafkaMessageStream(vertx, context, topic, stream, kafkaOffsets);
                        kafkaMessageStreams.add(messageStream);
                        context.runOnContext(v -> delegate.handle(messageStream));
                    });


                    future.complete();
                } catch (Exception e) {
                    future.fail(e);
                }
            }, false, asyncResult -> {
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


        public void stop(AsyncResultHandler<Void> stopHandler) {

            Observable.from(kafkaMessageStreams)
                    .flatMap(kafkaMessageStream -> {
                        final ObservableFuture<Void> stopFuture = RxHelper.observableFuture();
                        kafkaMessageStream.stopProcessing(stopFuture.toHandler());
                        return stopFuture;
                    })
                    .last()
                    .flatMap(v -> {
                        final ObservableFuture<Void> offsetStop = RxHelper.observableFuture();
                        if (kafkaOffsets != null) {
                            kafkaOffsets.shutdown(offsetStop.toHandler());
                        }
                        final ObservableFuture<Void> connStop = RxHelper.observableFuture();
                        vertx.executeBlocking(future -> {
                            try {
                                if (connector != null) {
                                    connector.shutdown();
                                }
                                future.complete();
                            } catch (Throwable throwable) {
                                future.fail(throwable);
                            }
                        }, connStop.toHandler());
                        return connStop.zipWith(offsetStop, (v1, v2) -> null);
                    })
                    .subscribe(v -> {
                        logger.info("[KafkaInput] - Stopped");
                        stopHandler.handle(Future.succeededFuture());
                    }, throwable -> stopHandler.handle(Future.failedFuture(throwable)));


        }
    }
}
