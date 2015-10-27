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
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

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

        kafkaLogListener.logHandler(stream -> {
            final DuplexStream<Buffer, Buffer> duplexStream = new DuplexStream<>(stream, NullClosableWriteStream.getInstance());

            duplexStream
                    .closeHandler(future -> stream.close())
                    .writeCompleteHandler(stream::commit);

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

        public KafkaLogListener(Vertx vertx, int numberOfThreads) {
            this.vertx = vertx;
            this.numberOfThreads = numberOfThreads;
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
                        List<String> brokers = Splitter.on(",").splitToList(configObj.getString(KEY_SEED_BROKERS));

                        KafkaOffsets kafkaOffsets = new KafkaOffsets(brokers, kafkaConsumerConfig.getGroupId());

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
            vertx.executeBlocking(future -> {
                kafkaMessageStreams.forEach(KafkaMessageStream::close);
                if (connector != null) {
                    connector.shutdown();
                }
                future.complete();
            }, asyncResult -> stopHandler.handle(Future.succeededFuture()));
        }
    }
}
