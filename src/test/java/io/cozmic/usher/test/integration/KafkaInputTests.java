package io.cozmic.usher.test.integration;

import com.cyberphysical.streamprocessing.verticles.KafkaProducerVerticle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import io.cozmic.usher.Start;
import io.cozmic.usher.test.Pojo;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static io.cozmic.usher.test.integration.EventBusFilter.EVENT_BUS_ADDRESS;
import static org.junit.Assert.fail;

/**
 * KafkaInputTests
 * Created by Craig Earley on 8/26/15.
 * Copyright (c) 2015 All Rights Reserved
 */
@RunWith(VertxUnitRunner.class)
public class KafkaInputTests {

    // IMPORTANT!
    // These tests require a local zookeepr instance running on port 2181
    // and a local Kafka instance running on port 9092.

    private static final Random random = new Random();
    private String topic = "test";
    private Vertx vertx;

    private static <T> byte[] serializedRecord(T object) {
        byte[] serializedObject = null;
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        try {
            mapper.acceptJsonFormatVisitor(object.getClass(), gen);
            serializedObject = mapper.writer(gen.getGeneratedSchema()).writeValueAsBytes(object);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return serializedObject;
    }

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();

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
    public void after(TestContext context) {
        final Async async = context.async();
        vertx.close(res -> {
            if (res.failed()) {
                context.fail(res.cause());
                return;
            }
            async.complete();
        });
    }

    /**
     * Test "Hello World!" -> Kafka -> KafkaInput -> EventBusFilter
     */
    @Test
    public void testConsumeRawMessage(TestContext context) throws Exception {

        final DeploymentOptions options = buildDeploymentOptions();

        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();

            // Given
            final String expected = "Hello World test 0" + random.nextInt(1_000);

            // When
            NetClient client = vertx.createNetClient();
            client.connect(1234, "localhost", res -> {
                if (res.succeeded()) {
                    NetSocket socket = res.result();
                    socket.handler(buffer -> {
                        String response = buffer.toString("UTF-8");
                        System.out.println("NetClient handled message: " + response);
                        client.close();
                    });
                    // Send message data to KafkaProducerVerticle
                    socket.write(expected);
                } else {
                    System.out.println("NetClient failed to connect");
                    client.close();
                    context.fail(res.cause());
                }
            });

            // Then
            AtomicInteger counter = new AtomicInteger(0);
            StringDeserializer stringDeserializer = new StringDeserializer();
            vertx.eventBus().<byte[]>consumer(EVENT_BUS_ADDRESS, msg -> {
                String message = stringDeserializer.deserialize("", msg.body());
                System.out.println(String.format("Received '%s'", message));
                if (counter.get() > 0) {
                    // Ignore subsequent messages to avoid calling
                    // async.complete() more than once.
                    return;
                }
                if (expected.equals(message)) {
                    counter.getAndIncrement();
                    // Give the plugin a couple of seconds to commit
                    vertx.setTimer(5_000, event -> async.complete());
                } else {
                    System.out.println(String.format("Expected '%s' but received '%s'", expected, message));
                }
            });
        }));
    }

    /**
     * Test Object -> Avro -> Kafka -> KafkaInput -> AvroDecoder -> EventBusFilter -> Object
     */
    @Test
    public void testConsumeAvroMessage(TestContext context) throws Exception {

        // TODO: This test is valid but it would be better to include the AvroDecoder as outlined above

        final DeploymentOptions options = buildDeploymentOptions();

        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();

            // Given
            Pojo user = new Pojo("Test", "#0" + random.nextInt(1_000), 1, "red");
            final byte[] expected = serializedRecord(user);

            // When
            NetClient client = vertx.createNetClient();
            client.connect(1234, "localhost", res -> {
                if (res.succeeded()) {
                    NetSocket socket = res.result();
                    socket.handler(buffer -> {
                        String response = buffer.toString("UTF-8");
                        System.out.println("NetClient handled message: " + response);
                        client.close();
                    });
                    // Send message data to KafkaProducerVerticle
                    socket.write(new String(expected));
                } else {
                    System.out.println("NetClient failed to connect");
                    client.close();
                    context.fail(res.cause());
                }
            });

            // Then
            AtomicInteger counter = new AtomicInteger(0);
            StringDeserializer stringDeserializer = new StringDeserializer();
            vertx.eventBus().<byte[]>consumer(EVENT_BUS_ADDRESS, msg -> {
                Buffer buffer = Buffer.buffer(msg.body());
                String message = stringDeserializer.deserialize("", msg.body());
                System.out.println(String.format("Received '%s'", message));
                if (counter.get() > 0) {
                    // Ignore subsequent messages to avoid calling
                    // async.complete() more than once.
                    return;
                }
                if (Arrays.equals(expected, buffer.getBytes())) {
                    counter.getAndIncrement();
                    // Give the plugin a couple of seconds to commit
                    vertx.setTimer(5_000, event -> async.complete());
                } else {
                    System.out.println(String.format("Expected '%s' but received '%s'", expected, message));
                }
            });
        }));
    }

    private DeploymentOptions buildDeploymentOptions() {
        JsonObject config = null;
        try {
            final URI uri = getClass().getResource("/config_journaling_input.json").toURI();
            final String configString = new String(Files.readAllBytes(Paths.get(uri)));
            config = new JsonObject(configString);
            config.getJsonObject("Router")
                    .put("zookeeper.connect", "zookeeper.dev:2181")
                    .put("topic", topic)
                    .put("group.id", "0")
                    .put("partition", 0)
                    .put("seed.brokers", new JsonArray().add("kafka.dev"))
                    .put("port", 9092);
        } catch (URISyntaxException | IOException e) {
            fail(e.getMessage());
        }
        final DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config);
        return options;
    }
}
