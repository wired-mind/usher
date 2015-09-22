package io.cozmic.usher.plugins.journaling;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndOffset;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * KafkaInputV2Tests
 * Created by Craig Earley on 9/21/15.
 * Copyright (c) 2015 All Rights Reserved
 */
@RunWith(VertxUnitRunner.class)
public class KafkaConsumerTests {

    private String topic = "test";
    private Vertx vertx;
    private KafkaConsumer kafkaconsumer;

    private void printMessageAndOffset(MessageAndOffset messageAndOffset) throws UnsupportedEncodingException {
        ByteBuffer payload = messageAndOffset.message().payload();
        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);
        System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
    }

    @Before
    public void before(TestContext context) throws Exception {
        vertx = Vertx.vertx();
        final JsonObject options = buildKafkaConsumerConfig();
        kafkaconsumer = new KafkaConsumerImpl(vertx, options);
    }

    @After
    public void after(TestContext context) throws Exception {
        if (kafkaconsumer != null) {
            kafkaconsumer.close();
        }
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testClose() throws Exception {
        kafkaconsumer.close();
    }

    @Test
    public void testCanCommitSingleOffset(TestContext context) throws Exception {
        final Async async = context.async();

        // TODO: Publish 2 test messages to kafka

        // Given
        final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);

        AsyncResult<Map<String, List<MessageAndOffset>>> future = kafkaconsumer.poll(topicAndPartition).get();

        Map<String, List<MessageAndOffset>> map = future.result();
        context.assertTrue(map.size() == 1, "map should have one entry");

        Map.Entry<String, List<MessageAndOffset>> entry = map.entrySet().iterator().next();
        context.assertTrue(topic.equals(entry.getKey()), "map entry key should match topic");

        List<MessageAndOffset> list = map.get(topicAndPartition.topic());
        context.assertTrue(list.size() > 0, "list of messages should not be empty");

        MessageAndOffset firstMessageAndOffset = list.get(0);
        printMessageAndOffset(firstMessageAndOffset);

        final long nextOffset = firstMessageAndOffset.offset() + 1;

        // When
        AsyncResult<Void> result = kafkaconsumer.commit(topicAndPartition, firstMessageAndOffset.offset()).get();

        // Then
        context.assertTrue(result.succeeded() && !result.failed(), "commit should have succeeded");

        kafkaconsumer.poll(topicAndPartition, res -> {
            if (res.failed()) {
                context.fail(res.cause());
            }
            List<MessageAndOffset> l = res.result().get(topicAndPartition.topic());
            context.assertEquals(nextOffset, l.get(0).offset(), "should be at the next offset");
            async.complete();
        });

        vertx.setTimer(5_000, event -> context.fail("Timeout"));
    }

    @Test
    public void testCanCommitMultipleOffsets(TestContext context) throws Exception {
        final Async async = context.async();

        // TODO: Publish 2 test messages to kafka

        final int numberOfTopics = 1;

        // Given
        final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);
        // TODO: Another topic and partition, subscribe to both

        kafkaconsumer.subscribe(topicAndPartition);
        AsyncResult<Map<String, List<MessageAndOffset>>> future = kafkaconsumer.poll().get();

        Map<String, List<MessageAndOffset>> map = future.result();
        context.assertTrue(map.size() == numberOfTopics, "map should have entry for each topic");

        Map.Entry<String, List<MessageAndOffset>> entry = map.entrySet().iterator().next();
        context.assertTrue(topic.equals(entry.getKey()), "map entry key should match topic");

        List<MessageAndOffset> list = map.get(topicAndPartition.topic());
        context.assertTrue(list.size() > 0, "list of messages should not be empty");

        MessageAndOffset firstMessageAndOffset = list.get(0);
        printMessageAndOffset(firstMessageAndOffset);

        // When
        Map<TopicAndPartition, Long> offsets = new LinkedHashMap();
        list.forEach(item -> offsets.put(topicAndPartition, item.offset()));

        AsyncResult<Void> result = kafkaconsumer.commit(offsets).get();

        // Then
        context.assertTrue(result.succeeded() && !result.failed(), "commit should have succeeded");
        kafkaconsumer.poll(res -> {
            context.fail("Should not have any result here");
        });
        // Wait and terminate manually
        vertx.setTimer(5_000, event -> async.complete());
    }

    @Test
    public void testCanPollSubscribedTopics(TestContext context) throws Exception {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);

        kafkaconsumer.subscribe(topicAndPartition);
        AsyncResult<Map<String, List<MessageAndOffset>>> future = kafkaconsumer.poll().get();

        Map<String, List<MessageAndOffset>> map = future.result();

        List<MessageAndOffset> list = map.get(topicAndPartition.topic());
        MessageAndOffset messageAndOffset = list.get(0);

        ByteBuffer payload = messageAndOffset.message().payload();
        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);

        context.assertNotNull(bytes, "bytes should not be null");
    }

    @Test
    public void testCanPollGivenTopic(TestContext context) throws Exception {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);

        AsyncResult<Map<String, List<MessageAndOffset>>> future = kafkaconsumer.poll(topicAndPartition).get();

        Map<String, List<MessageAndOffset>> map = future.result();

        List<MessageAndOffset> list = map.get(topicAndPartition.topic());
        MessageAndOffset messageAndOffset = list.get(0);

        ByteBuffer payload = messageAndOffset.message().payload();
        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);

        context.assertNotNull(bytes, "bytes should not be null");
    }

    @Test
    public void testSubscribe() throws Exception {
    }

    @Test
    public void testUnsubscribe() throws Exception {
    }

    private JsonObject buildKafkaConsumerConfig() {
        JsonObject config = new JsonObject();
        config.put("zookeeper.connect", "localhost:2181")
                .put("topic", topic)
                .put("group.id", "0")
                .put("partition", 0)
                .put("seed.brokers", new JsonArray().add("localhost"))
                .put("port", 9092);
        return config;
    }
}