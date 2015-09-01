package io.cozmic.usher.test.integration;

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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

/**
 * AvroTests
 * Created by Craig Earley on 8/19/15.
 * Copyright (c) 2015 All Rights Reserved
 */
@RunWith(VertxUnitRunner.class)
public class AvroTests {

    Vertx vertx;
    String externalSchema;
    AvroSchema jacksonSchema;
    AvroMapper mapper = new AvroMapper();

    @Before
    public void before(TestContext context) throws IOException {
        vertx = Vertx.vertx();
        
        jacksonSchema = mapper.schemaFor(Pojo.class);
        
        externalSchema = Resources.toString(Resources.getResource("avro/pojo.avsc"), Charsets.UTF_8);

        vertx.deployVerticle(RawEchoChamber.class.getName(), context.asyncAssertSuccess());
    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testCanDoSimpleAvroPojoFilter(TestContext context) throws IOException {
        final Async async = context.async();

        String color = "red";
        String expectedColor = "green";

        final DeploymentOptions options = new DeploymentOptions();

        final JsonObject config = new JsonObject();

        config
                .put("Router", buildInput())
                .put("AvroPojoFilter", buildAvroFilter("io.cozmic.usher.test.integration.AvroPojoFilter"))
                .put("AvroEncoder", buildAvroEncoder("AvroEncoder"))
                .put("AvroDecoder", buildAvroDecoder("AvroDecoder"));
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {

            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                // Create serialized User object
            	byte[] serializedObject = null;
                try {
					serializedObject = mapper.writer(jacksonSchema).writeValueAsBytes(new Pojo("Test", "#000-0000", 1, color));
                } catch (IOException e) {
                    e.printStackTrace();
                    context.fail("failed to encode User object");
                }

                final NetSocket socket = asyncResult.result();

                // Write serialized data to socket
                socket.write(Buffer.buffer(serializedObject));
                socket.handler(buffer -> {
                    /*
                     * By default, first 4 bytes is message length
                     * See {@link io.cozmic.usher.pipeline.OutPipelineFactoryImpl}
                     */
                    String buf = buffer.getBuffer(4, buffer.length()).toString();

                    Pojo user = new Pojo();
                    try {
                        user = mapper.reader(Pojo.class).with(jacksonSchema).readValue(buf.getBytes());
                    } catch (IOException e) {
                        e.printStackTrace();
                        context.fail("failed to decode data");
                    }
                    context.assertEquals(user.getFavoriteColor(), expectedColor);
                    context.assertEquals(user.getName(), "changed");
                    async.complete();
                });
            });
            vertx.setTimer(5000, event -> context.fail("timed out"));
        }));
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

    private JsonObject buildAvroEncoder(String type) {
        return new JsonObject().put("type", type).put("Schema",externalSchema);
    }

    private JsonObject buildAvroDecoder(String type) {
        return new JsonObject().put("type", type).put("Schema",externalSchema)
        		.put("avro", new JsonObject().put("type", "io.cozmic.usher.test.Pojo")
        						.put("schema", externalSchema));
    }

    private JsonObject buildAvroFilter(String type) {
        return new JsonObject().put("type", type).put("messageMatcher", "#{1==1}");
    }

    private JsonObject buildInput() {
        return new JsonObject().put("type", "TcpInput").put("host", "localhost").put("port", 2500).put("decoder", "AvroDecoder").put("encoder", "AvroEncoder");
    }
}
