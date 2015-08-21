package io.cozmic.usher.test.integration;

import io.cozmic.usher.RawEchoChamber;
import io.cozmic.usher.Start;
import io.cozmic.usher.test.User;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * AvroTests
 * Created by Craig Earley on 8/19/15.
 * Copyright (c) 2015 All Rights Reserved
 */
@RunWith(VertxUnitRunner.class)
public class AvroTests {

    Vertx vertx;
    String avroSchema;

    @Before
    public void before(TestContext context) throws IOException {
        vertx = Vertx.vertx();
        avroSchema = User.getClassSchema().toString();

        vertx.deployVerticle(RawEchoChamber.class.getName(), context.asyncAssertSuccess());
    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testCanDoSimpleAvroFilter(TestContext context) throws IOException {
        final Async async = context.async();

        String color = "red";
        String expectedColor = "green";

        final DeploymentOptions options = new DeploymentOptions();

        final JsonObject config = new JsonObject();
        final JsonObject avroFilter = buildAvroFilter();

        config
                .put("Router", buildInput())
                .put("AvroUserFilter", avroFilter)
                .put("AvroEncoder", buildAvroEncoder())
                .put("AvroDecoder", buildAvroDecoder());
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {

            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                // Create serialized User object
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

                DatumWriter<User> writer = new SpecificDatumWriter<>(User.getClassSchema());
                try {
                    writer.write(new User("Test", "#000-0000", 1, color), encoder);
                    encoder.flush();
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    context.fail("failed to encode User object");
                }

                final NetSocket socket = asyncResult.result();

                // Write serialized data to socket
                socket.write(new String(out.toByteArray(), Charset.forName("UTF8")));
                socket.handler(buffer -> {
                    /*
                     * By default, first 4 bytes is message length
                     * See {@link io.cozmic.usher.pipeline.OutPipelineFactoryImpl}
                     */
                    String buf = buffer.getBuffer(4, buffer.length()).toString();

                    // Get User object from serialized data
                    SpecificDatumReader<User> reader = new SpecificDatumReader<>(User.getClassSchema());

                    Decoder decoder = DecoderFactory.get().binaryDecoder(buf.getBytes(), null);
                    User user = new User();
                    try {
                        user = reader.read(null, decoder);
                    } catch (IOException e) {
                        e.printStackTrace();
                        context.fail("failed to decode data");
                    }
                    context.assertEquals(user.getFavoriteColor(), expectedColor);
                    async.complete();
                });
            });
            vertx.setTimer(5000, event -> context.fail("timed out"));
        }));
    }

    private JsonObject buildAvroEncoder() {
        return new JsonObject().put("type", "AvroEncoder").put("Schema", avroSchema);
    }

    private JsonObject buildAvroDecoder() {
        return new JsonObject().put("type", "AvroDecoder").put("Schema", avroSchema);
    }

    private JsonObject buildAvroFilter() {
        return new JsonObject().put("type", "io.cozmic.usher.test.integration.AvroUserFilter").put("messageMatcher", "#{1==1}");
    }

    private JsonObject buildInput() {
        return new JsonObject().put("type", "TcpInput").put("host", "localhost").put("port", 2500).put("decoder", "AvroDecoder").put("encoder", "AvroEncoder");
    }

}
