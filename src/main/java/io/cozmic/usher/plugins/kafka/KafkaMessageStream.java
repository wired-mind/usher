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
    private final KafkaReplyHandler replyHandler;
    private Handler<Buffer> readHandler;
    private boolean isPaused;
    private Handler<Buffer> delegate = null;
    private ConcurrentLinkedQueue<Buffer> readBuffers = new ConcurrentLinkedQueue<>();

    public KafkaMessageStream(Buffer buffer, TopicAndPartition topicAndPartition, MessageAndOffset messageAndOffset, KafkaCommitHandler commitHandler, KafkaReplyHandler replyHandler) {
        this.data = buffer;
        this.readBuffers.add(data);
        this.commitHandler = commitHandler;
        this.replyHandler = replyHandler;
        this.topicAndPartition = topicAndPartition;
        this.messageAndOffset = messageAndOffset;
    }

    public KafkaMessageStream responseHandler(Handler<Buffer> delegate) {
        this.delegate = delegate;
        return this;
    }

    public void close() {
        logger.info("Closing KafkaMessageStream");
    }

    public void commit(PipelinePack pack) {
        if (commitHandler == null) {
            return;
        }
        commitHandler.handle(topicAndPartition, messageAndOffset.offset(), asyncResult -> {
            if (asyncResult.failed()) {
                logger.error("Commit failed", asyncResult.cause());
                return;
            }

            if (replyHandler != null) replyHandler.handle(data.getBytes(), event -> {
                // TODO: When event.failed()?
            });
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
