package io.cozmic.usher.test.integration;

import com.cyberphysical.streamprocessing.verticles.KafkaProducerVerticle;
import io.cozmic.usher.plugins.journaling.KafkaConsumer;
import io.cozmic.usher.plugins.journaling.KafkaConsumerImpl;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
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
 * KafkaConsumerTests
 * Created by Craig Earley on 9/21/15.
 * Copyright (c) 2015 All Rights Reserved
 */
@RunWith(VertxUnitRunner.class)
public class KafkaConsumerTests {

    // IMPORTANT!
    // These tests require a local zookeepr instance running on port 2181
    // and a local Kafka instance running on port 9092.

    private String topic = "test";
    private Vertx vertx;
    private KafkaConsumer kafkaconsumer;

    private void printMessageAndOffset(MessageAndOffset messageAndOffset) {
        try {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private void messageToKafkaEx(String msg, Handler<AsyncResult<Void>> asyncResultHandler) {
        NetClient client = vertx.createNetClient();
        client.connect(1234, "localhost", res -> {
            if (res.failed()) {
                System.out.println("NetClient failed to connect");
                asyncResultHandler.handle(Future.failedFuture(res.cause()));
                return;
            }
            NetSocket socket = res.result();
            socket.handler(buffer -> {
                String response = buffer.toString("UTF-8");
                System.out.println("NetClient handled message: " + response);
                client.close();
            });
            // Send message data to KafkaProducerVerticle
            socket.write(msg);
            asyncResultHandler.handle(Future.succeededFuture());
        });
    }

    @Before
    public void before(TestContext context) throws Exception {
        vertx = Vertx.vertx();
        final JsonObject options = buildKafkaConsumerConfig();
        kafkaconsumer = new KafkaConsumerImpl(vertx, options);

        final Async async = context.async();

        vertx.deployVerticle(KafkaProducerVerticle.class.getName(),
                new DeploymentOptions().setConfig(new JsonObject()
                        .put("bootstrap.servers", "kafka.dev:" + 9092)
                        .put("topic", topic)
                        .put("tcpHost", "localhost")), event -> {

                    async.complete();
                });
    }

    @After
    public void after(TestContext context) throws Exception {
        final Async async = context.async();
        if (kafkaconsumer != null) {
            kafkaconsumer.close();
        }
        vertx.close(res -> {
            if (res.failed()) {
                context.fail(res.cause());
                return;
            }
            async.complete();
        });
    }

    @Test
    public void testClose() throws Exception {
        kafkaconsumer.close();
    }

    @Test
    public void testCanCommitSingleOffset(TestContext context) throws Exception {
        final Async async = context.async();

        messageToKafkaEx("message 1", message1 -> {
            if (message1.failed()) {
                System.out.println("Failed to send test message to Kafka");
                context.fail(message1.cause());
                return;
            }

            messageToKafkaEx("message 2", message2 -> {
                if (message2.failed()) {
                    System.out.println("Failed to send test message to Kafka");
                    context.fail(message2.cause());
                    return;
                }

                // Given
                final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);

                kafkaconsumer.poll(topicAndPartition, future -> {
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
                    kafkaconsumer.commit(topicAndPartition, firstMessageAndOffset.offset(), result -> {
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
                    });
                });
            });
        });
        vertx.setTimer(10_000, event -> context.fail("timed out"));
    }

    @Test
    public void testCanCommitMultipleOffsets(TestContext context) throws Exception {
        final Async async = context.async();

        messageToKafkaEx("message 3", message3 -> {
            if (message3.failed()) {
                System.out.println("Failed to send test message to Kafka");
                context.fail(message3.cause());
                return;
            }

            messageToKafkaEx("message 4", message4 -> {
                if (message4.failed()) {
                    System.out.println("Failed to send test message to Kafka");
                    context.fail(message4.cause());
                    return;
                }

                final int numberOfTopics = 1;

                // Given
                final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);
                // TODO: Another topic and partition, subscribe to both

                kafkaconsumer.poll(topicAndPartition, future -> {
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

                    kafkaconsumer.commit(offsets, result -> {
                        // Then
                        context.assertTrue(result.succeeded() && !result.failed(), "commit should have succeeded");
                        kafkaconsumer.poll(topicAndPartition, res -> context.fail("Should not have any result here"));
                    });
                });
            });
        });
        // Wait and terminate manually
        vertx.setTimer(5_000, event -> async.complete());
    }

    @Test
    public void testCanPollGivenTopic(TestContext context) throws Exception {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);

        messageToKafkaEx("message 5", context.asyncAssertSuccess(r -> {

            final Async async = context.async();
            kafkaconsumer.poll(topicAndPartition, res -> {
                if (res.failed()) {
                    context.fail(res.cause());
                    return;
                }
                Map<String, List<MessageAndOffset>> map = res.result();
                List<MessageAndOffset> list = map.get(topicAndPartition.topic());
                MessageAndOffset messageAndOffset = list.get(0);

                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);

                context.assertNotNull(bytes, "bytes should not be null");
                async.complete();
            });
        }));
        vertx.setTimer(10_000, event -> context.fail("timed out"));
    }

    private JsonObject buildKafkaConsumerConfig() {
        JsonObject config = new JsonObject();
        config.put("zookeeper.connect", "zookeeper.dev:2181")
                .put("topic", topic)
                .put("group.id", "0")
                .put("partition", 0)
                .put("seed.brokers", new JsonArray().add("kafka.dev"))
                .put("port", 9092);
        return config;
    }
}