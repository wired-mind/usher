package io.cozmic.usher.plugins.kafka;

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

/**
 * KafkaMessageStream
 * Created by Craig Earley on 10/6/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaMessageStream implements ReadStream<Buffer> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageStream.class.getName());
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
            } catch (Throwable throwable) {
                logger.error(throwable.getMessage(), throwable);
                if (exceptionHandler != null) context.runOnContext(v-> exceptionHandler.handle(throwable));
            }

        };

        consumerThread = new Thread(consumerRunnable);
        consumerThread.start();
    }


    public void close() {
        logger.info("KafkaMessageStream - Close requested. Kafka streams don't actually close. Instead this is " +
                "interpreted to mean that there was a problem processing the last message. For now we're just going to " +
                "commit and abandon that message. However, we plan to add a dead-letter-queue type feature here at " +
                "some point. Typically this will only occur when there are decoding errors. It could also happen if an " +
                "error strategy is setup that allows the error to bubble back. In most cases though we intend to " +
                "explicitly setup error strategies that will ensure processing.");

        //TODO: Add dead letter queue feature
        commit(WriteCompleteFuture.future(null));
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
        logger.info("Waiting to finish processing " + readBuffers.size() + " messages in " + topic);
        vertx.setTimer(1000, timerId -> {
            if (readBuffers.size() == 0) {
                stopHandler.handle(Future.succeededFuture());
                return;
            }

            doWaitOnStop(stopHandler);
        });
    }


    public void commit(WriteCompleteFuture future) {

        final long offset = this.currentMessage.offset() + 1;

        final int partition = this.currentMessage.partition();
        final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);


        logger.debug("Consumed: from " + topic + " at offset " + offset + " on thread: " + Thread.currentThread().getName());


        // Commit offset for this topic and partition
        kafkaOffsets.commitOffset(topicAndPartition, offset, asyncResult -> {
            if (asyncResult.failed()) {
                future.fail(asyncResult.cause());
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
        this.endHandler = endHandler;
        return this;
    }
}
