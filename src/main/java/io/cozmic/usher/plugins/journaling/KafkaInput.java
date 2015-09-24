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
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Connects to a Kafka broker and consumes messages
 * from the specified topic and partition.
 * <p>
 * Created by Craig Earley on 8/26/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaInput implements InputPlugin {
    private static final String TOPIC = "topic";
    private static final String PARTITION = "partition";
    private static final String CONSUMER_ADDRESS = KafkaInput.class.getName();
    private static final String EVT_RUN = "run";
    private static final Logger logger = LoggerFactory.getLogger(KafkaInput.class.getName());
    private JsonObject configObj;
    private Vertx vertx;
    private KafkaLogListener kafkaLogListener;

    @Override
    public void run(AsyncResultHandler<Void> startupHandler, Handler<DuplexStream<Buffer, Buffer>> duplexStreamHandler) {

        kafkaLogListener.logHandler(stream -> duplexStreamHandler.handle(new DuplexStream<>(stream, stream, pack -> {
            final Message message = pack.getMessage();
            // Data should enter the pipeline here.
            message.setLocalAddress(EmptyAddress.emptyAddress());
            message.setRemoteAddress(EmptyAddress.emptyAddress());
        }, v -> stream.close())));

        String topic = configObj.getString(TOPIC);
        Integer partition = configObj.getInteger(PARTITION, 0);

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

        kafkaLogListener.listen(topicAndPartition, asyncResult -> {
            if (asyncResult.failed()) {
                startupHandler.handle(Future.failedFuture(asyncResult.cause()));
                return;
            }
            logger.info("KafkaLogListener started: " + configObj);
            startupHandler.handle(Future.succeededFuture());
        });
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {
        this.configObj = configObj;
        this.vertx = vertx;
        kafkaLogListener = new KafkaLogListener(vertx, configObj);
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

    private class KafkaLogListener extends KafkaConsumerImpl {
        private Handler<KafkaMessageStream> delegate = null;

        public KafkaLogListener(Vertx vertx, JsonObject config) {
            super(vertx, config);
        }

        private KafkaLogListener logHandler(Handler<KafkaMessageStream> delegate) {
            this.delegate = delegate;
            return this;
        }

        private KafkaLogListener listen(TopicAndPartition topicAndPartition, Handler<AsyncResult<KafkaLogListener>> listenHandler) {
            // Setup listener event loop
            run(topicAndPartition);

            boolean succeeded = true;
            if (!succeeded) {
                listenHandler.handle(Future.failedFuture("Unable to start Kafka log listener"));
                return this;
            }
            listenHandler.handle(Future.succeededFuture());
            return this;
        }

        private void run(TopicAndPartition topicAndPartition) {
            vertx.eventBus().consumer(CONSUMER_ADDRESS, msg -> {
                if (!msg.body().equals(EVT_RUN)) {
                    return;
                }
                poll(topicAndPartition, future -> {
                    // Note: async result arrives only when data is available
                    if (future.failed()) {
                        logger.fatal("poll should wait until data is available and not return errors");
                        vertx.eventBus().publish(CONSUMER_ADDRESS, EVT_RUN);
                        return;
                    }
                    Map<String, List<MessageAndOffset>> map = future.result();

                    List<MessageAndOffset> list = map.get(topicAndPartition.topic());

                    for (MessageAndOffset messageAndOffset : list) {
                        ByteBuffer payload = messageAndOffset.message().payload();
                        byte[] bytes = new byte[payload.limit()];
                        payload.get(bytes);

                        if (delegate != null) {
                            KafkaMessageStream messageStream = new KafkaMessageStream(Buffer.buffer(bytes));

                            messageStream.responseHandler(data -> {
                                logger.warn("responseHandler not fully implemented; only committing offset.");

                                // TODO: From config [optional] response or reply topic

                                // Commit offset for this topic and partition (idempotent)
                                commit(topicAndPartition, messageAndOffset.offset());
                            });
                            messageStream.pause();
                            delegate.handle(messageStream);
                        }
                    }
                    // Once we handle all the data we can poll again
                    vertx.eventBus().publish(CONSUMER_ADDRESS, EVT_RUN);
                });
            });
            // Initial poll
            vertx.eventBus().publish(CONSUMER_ADDRESS, EVT_RUN);
        }
    }

    private class KafkaMessageStream implements ReadStream<Buffer>, WriteStream<Buffer> {
        private final Buffer data;
        private Handler<Buffer> handler;
        private boolean isPaused;
        private Handler<Buffer> delegate = null;
        private ConcurrentLinkedQueue<Buffer> readBuffers = new ConcurrentLinkedQueue<>();

        private KafkaMessageStream() {
            this.data = null;
        }

        public KafkaMessageStream(Buffer data) {
            this.data = data;
            this.readBuffers.add(this.data);
        }

        public KafkaMessageStream responseHandler(Handler<Buffer> delegate) {
            this.delegate = delegate;
            return this;
        }

        public void close() {
            // TODO: Is there anything else to do here?
            logger.info("Closing KafkaMessageStream");
        }

        protected void purgeReadBuffers() {
            while (!readBuffers.isEmpty() && !isPaused) {
                final Buffer nextPack = readBuffers.poll();
                if (nextPack != null) {
                    if (handler != null) handler.handle(nextPack);
                }
            }
        }

        @Override
        public KafkaMessageStream exceptionHandler(Handler<Throwable> handler) {
            return this;
        }

        @Override
        public WriteStream<Buffer> write(Buffer data) {
            if (this.delegate != null) {
                this.delegate.handle(data);
            }
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
            if (handler != null) {
                this.handler = handler;
                purgeReadBuffers();
            }
            return null;
        }

        @Override
        public ReadStream<Buffer> pause() {
            isPaused = true;
            return this;
        }

        @Override
        public ReadStream<Buffer> resume() {
            isPaused = false;
            purgeReadBuffers();
            return this;
        }

        @Override
        public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
            return this;
        }
    }
}
