package io.cozmic.usher.test.integration.plugins.kafka.simple;

import io.cozmic.usher.plugins.kafka.simple.KafkaConsumer;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import kafka.common.TopicAndPartition;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.util.Random;

/**
 * KafkaConsumerMock
 * Created by Craig Earley on 10/1/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaConsumerMock implements KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerMock.class.getName());
    private static final Random random = new Random();

    public KafkaConsumerMock(JsonObject config) {

    }

    /**
     * Simulates a random delay between 0.5 and 2 seconds.
     */
    private static void randomDelay() {
        int delay = 500 + random.nextInt(2_000);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        randomDelay();
    }

    @Override
    public void commit(TopicAndPartition topicAndPartition, Long offset) throws Exception {
        randomDelay();
    }

    @Override
    public MessageAndOffset poll(TopicAndPartition topicAndPartition) {
        randomDelay();

        Buffer buffer = Buffer.buffer(String.format("Thread {%s} - Topic {%s} - Partition {%s}",
                Thread.currentThread().getName(),
                topicAndPartition.topic(),
                topicAndPartition.partition()));

        return new MessageAndOffset(new Message(buffer.getBytes()), -1);
    }
}
