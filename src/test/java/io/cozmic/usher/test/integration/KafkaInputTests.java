package io.cozmic.usher.test.integration;

import com.google.common.io.Resources;
import io.cozmic.usher.Start;
import io.cozmic.usher.core.AvroMapper;
import io.cozmic.usher.test.Pojo;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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


        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.put("bootstrap.servers", "kafka.dev:9092");
        kafkaProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<>(kafkaProducerProps);
    }

    @After
    public void after(TestContext context) {

        producer.close();
        vertx.close(context.asyncAssertSuccess());
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
                if (!actualHashCode.equals(expectedHashCode)) {
                    context.fail("Pojo hashcodes do not match");
                    logger.info("Pojo hashcodes do not match");
                    logger.info("expected: " + expectedHashCode + " actual: " + actualHashCode);
                }
            });


            vertx.eventBus().<Integer>consumer("Router_complete", msg -> {
                async.complete();
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

        final Async async = context.async();
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {


            // Then


            vertx.eventBus().<Integer>consumer(EVENT_BUS_ADDRESS, msg -> {
                final Integer actualHashCode = msg.body();
                if (!actualHashCode.equals(expectedHashCode)) {
                    context.fail("Pojo hashcodes do not match");
                    logger.info("Pojo hashcodes do not match");
                    logger.info("expected: " + expectedHashCode + " actual: " + actualHashCode);
                }
            });

            vertx.eventBus().<Integer>consumer("Router_complete", msg -> {
                async.complete();

            });

            // When - publish to kafka
            vertx.executeBlocking(new Handler<Future<Void>>() {
                @Override
                public void handle(Future<Void> future) {
                    producer.send(data, (metadata, exception) -> {
                        future.complete();
                    });
                }
            }, context.asyncAssertSuccess());

        }));
        vertx.setTimer(15_000, event -> {
            context.fail("timed out");
        });
    }

    /**
     * Purpose of test: ErrFilter is configured to throw an error on the first message, but then start working.
     * <p>
     * The Usher FullDuplexMuxChannel is designed to stop and close the input stream if there is an error. With TCP
     * this closes the socket. Our desired KafkaInput behavior is that the message get sent to a DLQ (not implemented yet)
     * and that the input restart the stream by creating a new channel. We can test this by sending 2 messages and getting
     * just the second response since the first will err.
     *
     * @param context
     * @throws Exception
     */
    @Test
    public void testAfterErrorKafkaStreamWillRestart(TestContext context) throws Exception {
        // Given

        ProducerRecord<String, byte[]> first = new ProducerRecord<>(topic, "first".getBytes());
        ProducerRecord<String, byte[]> second = new ProducerRecord<>(topic, "second".getBytes());


        // When - publish to kafka
        vertx.executeBlocking(new Handler<Future<Void>>() {
            @Override
            public void handle(Future<Void> future) {
                producer.send(first, (metadata, exception) -> {
                    future.complete();
                });
            }
        }, context.asyncAssertSuccess());

        vertx.executeBlocking(new Handler<Future<Void>>() {
            @Override
            public void handle(Future<Void> future) {
                producer.send(second, (metadata, exception) -> {
                    future.complete();
                });
            }
        }, context.asyncAssertSuccess());


        final Async async = context.async();
        vertx.eventBus().<Integer>consumer("Router_complete", msg -> {

            async.complete();

        });
        final DeploymentOptions options = buildDeploymentOptions("/config_kafka_restart.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess());
        vertx.setTimer(5_000, event -> context.fail("timed out"));
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
