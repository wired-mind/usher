package io.cozmic.usher.plugins.kafka;

import com.google.common.collect.ImmutableMap;
import io.cozmic.usher.streams.WriteCompleteFuture;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import kafka.common.TopicAndPartition;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * KafkaMessageStream
 * Created by Craig Earley on 10/6/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaMessageStream implements ReadStream<Buffer> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageStream.class.getName());
    public static final long COMMIT_RETRY_ATTEMPTS = 3;
    private final Vertx vertx;
    private final String topic;
    private final KafkaOffsets kafkaOffsets;
    private Handler<Buffer> readHandler;
    private boolean isPaused;
    private ConcurrentLinkedQueue<MessageAndMetadata<byte[], byte[]>> readBuffers = new ConcurrentLinkedQueue<>();
    private Handler<Void> endHandler;
    private Handler<Throwable> exceptionHandler;
    private MessageAndMetadata<byte[], byte[]> currentMessage;
    private boolean stopped;
    private Thread consumerThread;

    public KafkaMessageStream(Vertx vertx, Context context, String topic, KafkaStream<byte[], byte[]> stream, KafkaOffsets kafkaOffsets) {
        this.vertx = vertx;

        this.topic = topic;
        this.kafkaOffsets = kafkaOffsets;

        Runnable consumerRunnable = () -> {
            logger.info("[Worker] - " + topic + " Starting in " + Thread.currentThread().getName());
            try {
                while (!stopped && stream.iterator().hasNext()) {
                    final MessageAndMetadata<byte[], byte[]> msg = stream.iterator().next();
                    readBuffers.add(msg);
                    context.runOnContext(v->purgeReadBuffers());
                }
                context.runOnContext(v->purgeReadBuffers());
                logger.info("[Worker] - " + topic + " Ending in " + Thread.currentThread().getName());
                if (endHandler != null) context.runOnContext(v -> endHandler.handle(null));
            }
            catch (Throwable throwable) {

                logger.error(throwable.getMessage(), throwable);
                if (exceptionHandler != null) context.runOnContext(v-> exceptionHandler.handle(throwable));
            }

        };

        consumerThread = new Thread(consumerRunnable);
        consumerThread.start();
    }


    public void stopProcessing(Handler<AsyncResult<Void>> stopHandler) {
        stopped = true;
        if (readBuffers.size() == 0) {
            stopHandler.handle(Future.succeededFuture());
            return;
        }
        doWaitOnStop(stopHandler);
    }

    private void doWaitOnStop(Handler<AsyncResult<Void>> stopHandler) {
        purgeReadBuffers();
        logger.info("Waiting to finish processing " + readBuffers.size() + " messages in " + topic + ". Paused: " + isPaused + " Current: " + currentMessage);
        vertx.setTimer(1000, timerId -> {
            if (readBuffers.size() == 0) {

                stopHandler.handle(Future.succeededFuture());
                return;
            }

            doWaitOnStop(stopHandler);
        });
    }


    /**
     * Will commit and retry up to 3 times if there is a problem
     * @param future
     */
    public void commit(WriteCompleteFuture<Void> future) {
        AtomicInteger retryCounter = new AtomicInteger();
        doCommit(future, retryCounter);
    }

    private void doCommit(WriteCompleteFuture<Void> future, AtomicInteger retryCounter) {
        final long offset = this.currentMessage.offset() + 1;

        final int partition = this.currentMessage.partition();
        final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);


        logger.debug("Consumed: from " + topic + " at offset " + offset + " on thread: " + Thread.currentThread().getName());


        // Commit offset for this topic and partition
        // Note: This can run more efficiently if offsets are batched
        kafkaOffsets.commitOffsets(ImmutableMap.of(topicAndPartition, offset), asyncResult -> {
            if (asyncResult.failed()) {
                final Throwable cause = asyncResult.cause();
                logger.error(cause.getMessage(), cause);
                final long attempt = retryCounter.incrementAndGet();

                if (attempt > COMMIT_RETRY_ATTEMPTS) {
                    logger.warn("[KafkaMessageStream] - Commit failed. Retry attempts exhausted.");
                    future.fail(cause);
                    return;
                }

                logger.warn("[KafkaMessageStream] - Commit failed, retry attempt " + attempt);
                doCommit(future, retryCounter);
                return;

            }

            logger.debug(String.format("committed %s at offset: %d", topicAndPartition, offset));
            this.currentMessage = null;
            purgeReadBuffers();
            future.complete();
        });
    }

    synchronized void purgeReadBuffers() {
        while (!readBuffers.isEmpty() && !isPaused && this.currentMessage == null) {
            final MessageAndMetadata<byte[], byte[]> msg = readBuffers.poll();
            if (msg != null) {
                if (readHandler != null) {
                    this.currentMessage = msg;
                    vertx.runOnContext(v -> readHandler.handle(Buffer.buffer(msg.message())));
                }
            }
        }
    }

    @Override
    public KafkaMessageStream exceptionHandler(Handler<Throwable> handler) {
        exceptionHandler = handler;
        return this;
    }

    @Override
    public ReadStream<Buffer> handler(Handler<Buffer> handler) {
        this.readHandler = handler;
        if (handler != null) {
            purgeReadBuffers();
        }
        return this;
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
        this.endHandler = endHandler;
        return this;
    }
}
