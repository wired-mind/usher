package io.cozmic.usher.plugins.kafka;

import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndOffset;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * KafkaMessageStream
 * Created by Craig Earley on 10/6/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaMessageStream implements ReadStream<Buffer> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageStream.class.getName());
    private final Buffer data;
    private final TopicAndPartition topicAndPartition;
    private final MessageAndOffset messageAndOffset;
    private final KafkaCommitHandler commitHandler;
    private Handler<Buffer> readHandler;
    private boolean isPaused;
    private Handler<Buffer> delegate = null;
    private ConcurrentLinkedQueue<Buffer> readBuffers = new ConcurrentLinkedQueue<>();

    public KafkaMessageStream(Buffer buffer, TopicAndPartition topicAndPartition, MessageAndOffset messageAndOffset, KafkaCommitHandler commitHandler) {
        this.data = buffer;
        this.readBuffers.add(data);
        this.commitHandler = commitHandler;
        this.topicAndPartition = topicAndPartition;
        this.messageAndOffset = messageAndOffset;
    }

    public KafkaMessageStream responseHandler(Handler<Buffer> delegate) {
        this.delegate = delegate;
        return this;
    }

    public void close() {
        logger.info("KafkaMessageStream - Close requested. Kafka streams don't actually close. Instead this is " +
                "interpreted to mean that there was a problem processing the last message. For now we're just going to " +
                "commit and abandon that message. However, we plan to add a dead-letter-queue type feature here at " +
                "some point. Typically this will only occur when there are decoding errors. It could also happen if an " +
                "error strategy is setup that allows the error to bubble back. In most cases though we intend to " +
                "explicitly setup error strategies that will ensure processing.");

        //TODO: Add dead letter queue feature
        commit();
    }

    public void commit() {
        if (commitHandler == null) {
            return;
        }
        commitHandler.handle(topicAndPartition, messageAndOffset.offset(), asyncResult -> {
            if (asyncResult.failed()) {
                logger.error("Commit failed", asyncResult.cause());
                return;
            }
        });
    }

    protected void purgeReadBuffers() {
        while (!readBuffers.isEmpty() && !isPaused) {
            final Buffer nextPack = readBuffers.poll();
            if (nextPack != null) {
                if (readHandler != null) readHandler.handle(nextPack);
            }
        }
    }

    @Override
    public KafkaMessageStream exceptionHandler(Handler<Throwable> handler) {
        return this;
    }

    @Override
    public ReadStream<Buffer> handler(Handler<Buffer> handler) {
        if (handler != null) {
            this.readHandler = handler;
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
