package io.cozmic.usher.test.integration;

import com.cyberphysical.streamprocessing.verticles.KafkaProducerVerticle;
import io.cozmic.usher.Start;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
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

import static io.cozmic.usher.test.integration.EventBusFilter.EVENT_BUS_ADDRESS;
import static org.junit.Assert.fail;

/**
 * KafkaInputV2Tests
 * Created by Craig Earley on 9/14/15.
 * Copyright (c) 2015 All Rights Reserved
 */
@RunWith(VertxUnitRunner.class)
public class KafkaInputV2Tests {
    private String topic = "test";
    private Vertx vertx;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();

        // IMPORTANT!
        // This test requires a local zookeepr instance running on port 2181
        // and a local Kafka instance running on port 9092.

        final Async async = context.async();

        vertx.deployVerticle(KafkaProducerVerticle.class.getName(),
                new DeploymentOptions().setConfig(new JsonObject()
                        .put("bootstrap.servers", "localhost:" + 9092)
                        .put("topic", topic)
                        .put("tcpHost", "localhost")), event -> {

                    async.complete();
                });
    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
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
            final String expected = "Hello World!";

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
            StringDeserializer stringDeserializer = new StringDeserializer();
            vertx.eventBus().<byte[]>consumer(EVENT_BUS_ADDRESS, msg -> {
                String message = stringDeserializer.deserialize("", msg.body());
                if (expected.equals(message)) {
                    async.complete();
                } else {
                    System.out.println(String.format("Received '%s'", message));
                    // context.fail(String.format("Expected '%s' but received '%s'", expected, message));
                }
            });
        }));
    }

    private DeploymentOptions buildDeploymentOptions() {
        JsonObject config = null;
        try {
            final URI uri = getClass().getResource("/config_journaling_inputV2.json").toURI();
            final String configString = new String(Files.readAllBytes(Paths.get(uri)));
            config = new JsonObject(configString);
            config.getJsonObject("Router")
                    .put("zookeeper.connect", "localhost:2181")
                    .put("topic", topic)
                    .put("group.id", "0")
                    .put("partition", 0)
                    .put("seed.brokers", new JsonArray().add("localhost"))
                    .put("port", 9092);
        } catch (URISyntaxException | IOException e) {
            fail(e.getMessage());
        }
        final DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config);
        return options;
    }
}
