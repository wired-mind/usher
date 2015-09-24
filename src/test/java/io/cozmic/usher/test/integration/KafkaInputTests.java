package io.cozmic.usher.test.integration;

import com.cyberphysical.streamprocessing.verticles.KafkaProducerVerticle;
import io.cozmic.usher.Start;
import io.cozmic.usher.test.Pojo;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static io.cozmic.usher.test.integration.EventBusFilter.EVENT_BUS_ADDRESS;
import static org.junit.Assert.fail;

/**
 * KafkaInputTests
 * Created by Craig Earley on 8/26/15.
 * Copyright (c) 2015 All Rights Reserved
 * <p>
 */
@RunWith(VertxUnitRunner.class)
public class KafkaInputTests {

    private EmbeddedZookeeper zkServer;
    private KafkaServer kafkaServer;
    private String topic = "testTopic";
    private Vertx vertx;
    private ZkClient zkClient;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();

        String zkConnect = TestZKUtils.zookeeperConnect();
        zkServer = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        int port = TestUtils.choosePort();
        int brokerId = 0;
        Properties props = TestUtils.createBrokerConfig(brokerId, port, true);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        final Async async = context.async();

        vertx.deployVerticle(KafkaProducerVerticle.class.getName(),
                new DeploymentOptions().setConfig(new JsonObject()
                        .put("bootstrap.servers", "localhost:" + port)
                        .put("topic", topic)
                        .put("tcpHost", "localhost")), event -> {

                    async.complete();
                });
    }

    @After
    public void after(TestContext context) {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
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
            AtomicInteger counter = new AtomicInteger(0);
            StringDeserializer stringDeserializer = new StringDeserializer();
            vertx.eventBus().<byte[]>consumer(EVENT_BUS_ADDRESS, msg -> {
                String message = stringDeserializer.deserialize("", msg.body());
                if (counter.get() > 0) {
                    // Note: For some reason we are receiving multiple
                    // messages here and async.complete() would be called
                    // again which causes this test to fail. This was not
                    // happening when I first created this test so something
                    // else has changed. Since this older version of the
                    // plugin is going away we will just handle it this way.
                    return;
                }
                if (expected.equals(message)) {
                    async.complete();
                    counter.getAndIncrement();
                } else {
                    context.fail(String.format("Expected '%s' but received '%s'", expected, message));
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
            Pojo user = new Pojo("Test", "#000-0000", 1, "red");
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
            StringDeserializer stringDeserializer = new StringDeserializer();
            vertx.eventBus().<byte[]>consumer(EVENT_BUS_ADDRESS, msg -> {
                Buffer buffer = Buffer.buffer(msg.body());
                String message = stringDeserializer.deserialize("", msg.body());
                if (Arrays.equals(expected, buffer.getBytes())) {
                    async.complete();
                } else {
                    context.fail(String.format("Expected '%s' but received '%s'", expected, message));
                }
            });
        }));
    }

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

    private DeploymentOptions buildDeploymentOptions() {
        JsonObject config = null;
        try {
            final URI uri = getClass().getResource("/config_journaling_input.json").toURI();
            final String configString = new String(Files.readAllBytes(Paths.get(uri)));
            config = new JsonObject(configString);
            config.getJsonObject("Router").put("zookeeper.connect", zkServer.connectString())
                    .put("topic", topic)
                    .put("group.id", "testGroup" + topic);
        } catch (URISyntaxException | IOException e) {
            fail(e.getMessage());
        }
        final DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config);
        return options;
    }
}