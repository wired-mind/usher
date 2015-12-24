package io.cozmic.usher.test.unit;

import io.cozmic.usher.Start;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import kafka.zk.EmbeddedZookeeper;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static io.cozmic.usher.test.integration.EventBusFilter.EVENT_BUS_ADDRESS;
import static org.junit.Assert.fail;

/**
 * EmbeddedKafkaTests
 * Created by Craig Earley on 12/14/15.
 * Copyright (c) 2015 All Rights Reserved
 */
@RunWith(VertxUnitRunner.class)
public class EmbeddedKafkaTests {
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedKafkaTests.class.getName());
    private static final Random random = new Random();

    private String topic = "test";
    private String groupId = "group0";

    private EmbeddedZookeeper zookeeper;
    private EmbeddedKafkaServer kafkaServer;
    private KafkaProducer<String, byte[]> producer;
    private long timeout = 60_000;
    private Vertx vertx;

    @Before
    public void before(TestContext context) throws Exception {
        vertx = Vertx.vertx();

        // Setup Zookeeper
        logger.info("Starting Zookeeper");
        zookeeper = new EmbeddedZookeeper("zookeeper.dev:" + kafka.utils.TestUtils.choosePort());

        final Async async = context.async();

        // Setup Kafka brokers
        List<Integer> kafkaPorts = new ArrayList<>();
        // -1 for any available port
        kafkaPorts.add(-1);
        kafkaPorts.add(-1);
        kafkaPorts.add(-1);
        final int replicationFactor = 2;
        final int numberOfPartitions = 5;
        Properties properties = new Properties();
        properties.setProperty("offsets.topic.replication.factor", String.valueOf(replicationFactor));
        properties.setProperty("zookeeper.connection.timeout.ms", "10000");
        properties.setProperty("num.partitions", String.valueOf(numberOfPartitions));

        logger.info("Starting Kafka cluster");
        kafkaServer = new EmbeddedKafkaServer(vertx, zookeeper.connectString(), properties, kafkaPorts);
        kafkaServer.start(asyncResult -> {

            logger.info("Kafka cluster started, setting up topics...");
            kafkaServer.createTopic(topic);
            System.out.println(kafkaServer.metaDataDump());
            System.out.println("Partition: " + 0 + ": lead broker is: " + kafkaServer.leadBroker(topic, 0));

            // Create Kafka producer
            Properties kafkaProducerProps = new Properties();
            kafkaProducerProps.put("bootstrap.servers", kafkaServer.getBootstrapServers());
            kafkaProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

            producer = new KafkaProducer<>(kafkaProducerProps);

            // deleting zookeeper information to make sure the consumer starts from the beginning
            // see https://stackoverflow.com/questions/14935755/how-to-get-data-from-old-offset-point-in-kafka
            kafkaServer.zkClient().delete("/consumers/" + groupId);

            async.complete();
        });
    }

    @After
    public void after(TestContext context) throws Exception {
        logger.info("shutting down");
        final Async async = context.async();

        producer.close();
        producer = null;

        kafkaServer.shutdownCluster(event -> {

        });

        zookeeper.shutdown();
        vertx.close(asyncResult -> async.complete());
    }

    /**
     * Test (data x n) -> Kafka -> KafkaInput (w/ leader shutdown at 1/2 n) -> EventBusFilter
     */
    @Test
    public void shouldConsumeAllMessageWhenLeaderIsChanged(TestContext context) throws Exception {
        final Async async = context.async();
        final AtomicInteger counter = new AtomicInteger();

        // Given
        final String expected = "Data 0" + random.nextInt(1_000);
        final int expectedHashCode = expected.hashCode();
        final int numberOfMessages = 1000;
        final ProducerRecord<String, byte[]> data = new ProducerRecord<>(topic, expected.getBytes());
        final Integer leadBrokerAtStart = kafkaServer.leadBroker(topic, 0);

        context.assertTrue(numberOfMessages % 2 == 0, "numberOfMessages should be an even number");

        final DeploymentOptions options = buildDeploymentOptions("/config_kafka_input_string.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {

            Handler<AsyncResult<Void>> leadBrokerShutdownHandler = event -> {
                for (int i = 0; i < numberOfMessages / 2; i++) {
                    producer.send(data, (metadata, exception) -> {
                        if (exception != null) {
                            context.fail(exception);
                        }
                    });
                }
            };

            // When - publish to kafka
            for (int i = 0; i < numberOfMessages / 2; i++) {
                producer.send(data, (metadata, exception) -> {
                    if (exception != null) {
                        context.fail(exception);
                    }
                });
            }

            // Then
            vertx.eventBus().<Integer>consumer(EVENT_BUS_ADDRESS, msg -> {
                final Integer actualHashCode = msg.body();
                if (actualHashCode.equals(expectedHashCode)) {
                    counter.incrementAndGet();
                    logger.info("received expected data, counter = " + counter.get());
                    if (counter.get() == numberOfMessages / 2) {
                        kafkaServer.shutdownBroker(leadBrokerAtStart, leadBrokerShutdownHandler);
                    } else if (counter.get() == numberOfMessages) {
                        Integer newLeadBroker = kafkaServer.leadBroker(topic, 0);
                        context.assertTrue(!newLeadBroker.equals(leadBrokerAtStart));
                        async.complete();
                    }
                } else {
                    context.fail("Pojo hashcodes do not match");
                    logger.info("Pojo hashcodes do not match");
                    logger.info("expected: " + expectedHashCode + " actual: " + actualHashCode);
                }
            });

        }));
        vertx.setTimer(timeout, event -> context.fail("timed out"));
    }

    /**
     * Test (data x n) -> Kafka -> KafkaInput (w/ zk shutdown at 1/2 n) -> EventBusFilter
     */
    @Test
    public void shouldConsumeAllMessageWhenZookeeperIsRestarted(TestContext context) throws Exception {
        final Async async = context.async();
        final AtomicInteger counter = new AtomicInteger();

        // Given
        final String expected = "Data 0" + random.nextInt(1_000);
        final int expectedHashCode = expected.hashCode();
        final int numberOfMessages = 1000;
        final ProducerRecord<String, byte[]> data = new ProducerRecord<>(topic, expected.getBytes());

        context.assertTrue(numberOfMessages % 2 == 0, "numberOfMessages should be an even number");

        final DeploymentOptions options = buildDeploymentOptions("/config_kafka_input_string.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {

            Handler<AsyncResult<Void>> zookeeperRestartHandler = event -> {
                for (int i = 0; i < numberOfMessages / 2; i++) {
                    producer.send(data, (metadata, exception) -> {
                        if (exception != null) {
                            context.fail(exception);
                        }
                    });
                }
            };

            // When - publish to kafka
            for (int i = 0; i < numberOfMessages / 2; i++) {
                producer.send(data, (metadata, exception) -> {
                    if (exception != null) {
                        context.fail(exception);
                    }
                });
            }

            // Then
            vertx.eventBus().<Integer>consumer(EVENT_BUS_ADDRESS, msg -> {
                final Integer actualHashCode = msg.body();
                if (actualHashCode.equals(expectedHashCode)) {
                    counter.incrementAndGet();
                    logger.info("received expected data, counter = " + counter.get());
                    if (counter.get() == numberOfMessages / 2) {
                        restartZookeeper(zookeeperRestartHandler);
                    } else if (counter.get() == numberOfMessages) {
                        async.complete();
                    }
                } else {
                    context.fail("Pojo hashcodes do not match");
                    logger.info("Pojo hashcodes do not match");
                    logger.info("expected: " + expectedHashCode + " actual: " + actualHashCode);
                }
            });

        }));
        vertx.setTimer(timeout, event -> context.fail("timed out"));
    }

    private DeploymentOptions buildDeploymentOptions(String name) {
        JsonObject config = null;
        try {
            final URI uri = getClass().getResource(name).toURI();
            final String configString = new String(Files.readAllBytes(Paths.get(uri)));
            config = new JsonObject(configString);
            config.getJsonObject("Router")
                    .put("zookeeper.connect", zookeeper.connectString())
                    .put("topic", topic)
                    .put("group.id", groupId)
                    .put("partitions", 5)
                    .put("seed.brokers", kafkaServer.getBootstrapServers());
        } catch (URISyntaxException | IOException e) {
            fail(e.getMessage());
        }
        final DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config);
        return options;
    }

    private void restartZookeeper(Handler<AsyncResult<Void>> asyncResult) {
        vertx.executeBlocking(future -> {
            try {
                String connectString = zookeeper.connectString();
                zookeeper.shutdown();
                Thread.sleep(5_000);
                logger.info("Restarting Zookeeper");
                zookeeper = new EmbeddedZookeeper(connectString);
            } catch (Exception e) {
                logger.error(e);
            }
            future.complete();
        }, asyncResult);
    }
}
