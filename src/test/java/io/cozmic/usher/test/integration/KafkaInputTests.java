package io.cozmic.usher.test.integration;

import com.google.common.io.Resources;
import io.cozmic.usher.Start;
import io.cozmic.usher.core.AvroMapper;
import io.cozmic.usher.test.Pojo;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;

import static io.cozmic.usher.test.integration.EventBusFilter.EVENT_BUS_ADDRESS;
import static org.junit.Assert.fail;

/**
 * KafkaInputTests
 * Created by Craig Earley on 10/2/15.
 * Copyright (c) 2015 All Rights Reserved
 */
@RunWith(VertxUnitRunner.class)
public class KafkaInputTests {

    // IMPORTANT!
    // These tests require a local zookeepr instance running on port 2181
    // and a local Kafka instance running on port 9092.
    private static final Logger logger = LoggerFactory.getLogger(KafkaInputTests.class.getName());
    private static final Random random = new Random();
    private String topic = "test";
    private Vertx vertx;
    private KafkaProducer<String, byte[]> producer;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();

        vertx.deployVerticle("", new DeploymentOptions().setWorker(true).setInstances(5), asyncResult -> {
            //
        });

        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.put("bootstrap.servers", "kafka.dev:9092");
        kafkaProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<>(kafkaProducerProps);
    }

    @After
    public void after(TestContext context) {
        final Async async = context.async();
        producer.close();
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
        // Given
        final String expected = "Hello World test 0" + random.nextInt(1_000);
        final int expectedHashCode = expected.hashCode();
        ProducerRecord<String, byte[]> data = new ProducerRecord<>(topic, expected.getBytes());


        final DeploymentOptions options = buildDeploymentOptions("/config_kafka_input_string.json");

        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();


            // When - publish to kafka
            vertx.executeBlocking(new Handler<Future<Void>>() {
                @Override
                public void handle(Future<Void> future) {
                    producer.send(data, (metadata, exception) -> {
                        future.complete();
                    });
                }
            }, context.asyncAssertSuccess());

            // Then
            vertx.eventBus().<Integer>consumer(EVENT_BUS_ADDRESS, msg -> {
                final Integer actualHashCode = msg.body();
                if (actualHashCode.equals(expectedHashCode)) {
                    async.complete();
                } else {
                    logger.info("expected: " + expectedHashCode + " actual: " + actualHashCode);
                }
            });
        }));
        vertx.setTimer(15_000, event -> context.fail("timed out"));
    }

    /**
     * Test Object -> Avro -> Kafka -> KafkaInput -> AvroDecoder -> EventBusFilter -> Object
     */
    @Test
    public void testConsumeAvroMessage(TestContext context) throws Exception {
        final AvroMapper avroMapper = new AvroMapper(Resources.getResource("avro/pojo.avsc"));
        Pojo user = new Pojo("Test", "#0" + random.nextInt(1_000), 1, "red");

        // Given
        final int expectedHashCode = user.hashCode();
        final byte[] payload = avroMapper.serialize(user);


        ProducerRecord<String, byte[]> data = new ProducerRecord<>(topic, payload);

        final DeploymentOptions options = buildDeploymentOptions("/config_journaling_input.json");

        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();

            // When - publish to kafka
            vertx.executeBlocking(new Handler<Future<Void>>() {
                @Override
                public void handle(Future<Void> future) {
                    producer.send(data, (metadata, exception) -> {
                        future.complete();
                    });
                }
            }, context.asyncAssertSuccess());

            // Then
            vertx.eventBus().<Integer>consumer(EVENT_BUS_ADDRESS, msg -> {
                final Integer actualHashCode = msg.body();
                if (actualHashCode.equals(expectedHashCode)) {
                    async.complete();
                } else {
                    logger.info("Pojo hashcodes do not match");
                    logger.info("expected: " + expectedHashCode + " actual: " + actualHashCode);
                }
            });
        }));
        vertx.setTimer(15_000, event -> context.fail("timed out"));
    }

    private DeploymentOptions buildDeploymentOptions(String name) {
        JsonObject config = null;
        try {
            final URI uri = getClass().getResource(name).toURI();
            final String configString = new String(Files.readAllBytes(Paths.get(uri)));
            config = new JsonObject(configString);
            config.getJsonObject("Router")
                    .put("zookeeper.connect", "zookeeper.dev:2181")
                    .put("topic", topic)
                    .put("group.id", "0")
                    .put("partitions", 5)
                    .put("seed.brokers", "kafka.dev:9092");
        } catch (URISyntaxException | IOException e) {
            fail(e.getMessage());
        }
        final DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config);
        return options;
    }
}
