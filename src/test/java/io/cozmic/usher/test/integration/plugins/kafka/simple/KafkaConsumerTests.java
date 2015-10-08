package io.cozmic.usher.test.integration.plugins.kafka.simple;

import com.cyberphysical.streamprocessing.verticles.KafkaProducerVerticle;
import io.cozmic.usher.plugins.kafka.simple.KafkaConsumer;
import io.cozmic.usher.plugins.kafka.simple.KafkaConsumerImpl;
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
        kafkaconsumer = new KafkaConsumerImpl(options);

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

                vertx.executeBlocking(future -> {
                    // Given
                    final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);

                    MessageAndOffset firstMessageAndOffset = kafkaconsumer.poll(topicAndPartition);

                    printMessageAndOffset(firstMessageAndOffset);

                    final long nextOffset = firstMessageAndOffset.offset() + 1;

                    // When
                    try {
                        kafkaconsumer.commit(topicAndPartition, firstMessageAndOffset.offset());
                    } catch (Exception e) {
                        future.fail("commit should have succeeded");
                        return;
                    }
                    // Then
                    MessageAndOffset nextMessageAndOffset = kafkaconsumer.poll(topicAndPartition);
                    if (nextOffset != nextMessageAndOffset.offset()) {
                        future.fail("should be at the next offset");
                        return;
                    }
                    future.complete();
                }, asyncResult -> {
                    if (asyncResult.failed()) {
                        context.fail();
                        return;
                    }
                    async.complete();
                });
            });
        });
        vertx.setTimer(10_000, event -> context.fail("timed out"));
    }

    @Test
    public void testCanPollGivenTopic(TestContext context) throws Exception {
        final Async async = context.async();

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);

        messageToKafkaEx("message 5", context.asyncAssertSuccess(r -> {

            vertx.executeBlocking(future -> {
                MessageAndOffset messageAndOffset = kafkaconsumer.poll(topicAndPartition);

                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);

                if (bytes == null) {
                    future.fail("bytes should not be null");
                    return;
                }
                future.complete();
            }, asyncResult -> {
                if (asyncResult.failed()) {
                    context.fail();
                    return;
                }
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
                .put("partitions", 1)
                .put("seed.brokers", new JsonArray().add("kafka.dev"))
                .put("port", 9092);
        return config;
    }
}