package io.cozmic.usher.test.integration;

import com.google.common.io.Resources;
import io.cozmic.usher.RawEchoChamber;
import io.cozmic.usher.Start;
import io.cozmic.usher.test.Pojo;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

/**
 * AvroTests
 * Created by Craig Earley on 8/19/15.
 * Copyright (c) 2015 All Rights Reserved
 */
@RunWith(VertxUnitRunner.class)
public class AvroTests {

    Vertx vertx;


    @Before
    public void before(TestContext context) throws IOException {
        vertx = Vertx.vertx();




        vertx.deployVerticle(RawEchoChamber.class.getName(), context.asyncAssertSuccess());
    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void missingSchemaShouldErr(TestContext context) throws IOException {
        final DeploymentOptions options = new DeploymentOptions();

        final JsonObject config = new JsonObject();

        config
                .put("Router", buildInput())
                .put("AvroPojoFilter", buildAvroFilter("io.cozmic.usher.test.integration.AvroPojoFilter"))
                .put("AvroEncoder", buildAvroEncoder("bogus", "io.cozmic.usher.test.Pojo"))
                .put("AvroDecoder", buildAvroDecoder("bogus", "io.cozmic.usher.test.Pojo"));
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertFailure());

        vertx.setTimer(5000, event -> context.fail("timed out"));
    }


    @Test
    public void testCanDoSimpleAvroPojoFilter(TestContext context) throws IOException {
        final Async async = context.async();

        String color = "red";
        String expectedColor = "green";

        // Create serialized User object
        final io.cozmic.usher.core.AvroMapper avroMapper = new io.cozmic.usher.core.AvroMapper(Resources.getResource("avro/pojo.avsc"));
        final Pojo pojo = new Pojo("Test", "#000-0000", 1, color);
        final byte[] serializedObject = avroMapper.serialize(pojo);

        final DeploymentOptions options = new DeploymentOptions();

        final JsonObject config = new JsonObject();

        config
                .put("Router", buildInput())
                .put("AvroPojoFilter", buildAvroFilter("io.cozmic.usher.test.integration.AvroPojoFilter"))
                .put("AvroEncoder", buildAvroEncoder("avro/pojo.avsc", "io.cozmic.usher.test.Pojo"))
                .put("AvroDecoder", buildAvroDecoder("avro/pojo.avsc", "io.cozmic.usher.test.Pojo"))
                .put("NullSplitterWithMsgBytes", buildNullSplitterWithMsgBytes());
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {

            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {



                final NetSocket socket = asyncResult.result();


                socket.handler(buffer -> {
                    /*
                     * By default, first 4 bytes is message length
                     * See {@link io.cozmic.usher.pipeline.OutPipelineFactoryImpl}
                     */
                    final Buffer frame = buffer.getBuffer(4, buffer.length());


                    try {
                        final Pojo user = avroMapper.deserialize(Pojo.class, frame.getBytes());
                        context.assertEquals(user.getFavoriteColor(), expectedColor);
                        context.assertEquals(user.getName(), "changed");
                        async.complete();
                    } catch (IOException e) {
                        context.fail("failed to decode data");
                    }


                });

                // Write serialized data to socket
                socket.write(Buffer.buffer(serializedObject));
            });
        }));
        vertx.setTimer(5000, event -> context.fail("timed out"));
    }

// Commenting out.  It does not appear easy to ser/deser to pojo and avro generic simultaneously
//    @Test
//    public void testCanDoSimpleAvroGenericFilter(TestContext context) throws IOException {
//        final Async async = context.async();
//
//        String color = "red";
//        String expectedColor = "green";
//
//        final DeploymentOptions options = new DeploymentOptions();
//
//        final JsonObject config = new JsonObject();
//
//        config
//                .put("Router", buildInput())
//                .put("AvroPGenericFilter", buildAvroFilter("io.cozmic.usher.test.integration.AvroGenericFilter"))
//                .put("AvroEncoder", buildAvroEncoder("GenericAvroEncoder"))
//                .put("AvroDecoder", buildAvroDecoder("GenericAvroDecoder"));
//        options.setConfig(config);
//        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
//
//            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
//                // Create serialized User object
//                ByteArrayOutputStream out = new ByteArrayOutputStream();
//                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
//
//                DatumWriter<Pojo> writer = new SpecificDatumWriter<>(new Schema.Parser().parse(externalSchema));
//                try {
//                    writer.write(new Pojo("Test", "#000-0000", 1, color), encoder);
//                    encoder.flush();
//                    out.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    context.fail("failed to encode User object");
//                }
//
//                final NetSocket socket = asyncResult.result();
//
//                // Write serialized data to socket
//                socket.write(new String(out.toByteArray(), Charset.forName("UTF8")));
//                socket.handler(buffer -> {
//                    /*
//                     * By default, first 4 bytes is message length
//                     * See {@link io.cozmic.usher.pipeline.OutPipelineFactoryImpl}
//                     */
//                    String buf = buffer.getBuffer(4, buffer.length()).toString();
//
//                    // Get User object from serialized data
//                    SpecificDatumReader<Pojo> reader = new SpecificDatumReader<>(new Schema.Parser().parse(externalSchema));
//
//                    Decoder decoder = DecoderFactory.get().binaryDecoder(buf.getBytes(), null);
//                    Pojo user = new Pojo();
//                    try {
//                        user = reader.read(null, decoder);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                        context.fail("failed to decode data");
//                    }
//                    context.assertEquals(user.getFavoriteColor(), expectedColor);
//                    async.complete();
//                });
//            });
//            vertx.setTimer(5000, event -> context.fail("timed out"));
//        }));
//    }

    private JsonObject buildAvroEncoder(String schema, String clazz) {
        return new JsonObject().put("schema", schema).put("clazz", clazz);
    }

    private JsonObject buildAvroDecoder(String schema, String clazz) {
        return new JsonObject().put("schema", schema)
                .put("clazz", clazz);
    }

    private JsonObject buildAvroFilter(String type) {
        return new JsonObject().put("type", type).put("messageMatcher", "#{1==1}");
    }


    private JsonObject buildNullSplitterWithMsgBytes() {
        return new JsonObject().put("type", "io.cozmic.usher.plugins.core.NullSplitter").put("useMessageBytes", true);
    }

    private JsonObject buildInput() {
        return new JsonObject().put("type", "TcpInput").put("host", "localhost").put("port", 2500).put("splitter","NullSplitterWithMsgBytes").put("decoder", "AvroDecoder").put("encoder", "AvroEncoder").put("messageMatcher", "#{1==1}");
    }
}
