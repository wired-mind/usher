package io.cozmic.usher.test.localintegration;


import io.cozmic.usher.RawEchoChamber;
import io.cozmic.usher.Start;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.fail;

/**
 * Created by chuck on 6/29/15.
 */
@RunWith(VertxUnitRunner.class)
public class SplitterTests {

    Vertx vertx;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();
        vertx.deployVerticle(RawEchoChamber.class.getName(), context.asyncAssertSuccess());

    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }


    @Test
    public void testCanEchoDelimitedStream(TestContext context) {
        final DeploymentOptions options = new DeploymentOptions();
        JsonObject inputConfig = buildInput();
        inputConfig.put("splitter", "NewLineSplitter");
        final JsonObject config = new JsonObject();
        config
                .put("usher", new JsonObject().put("pipelineInstances", 1))
                .put("PayloadEncoder", new JsonObject())
                .put("Router", inputConfig)
                .put("EchoBackend", buildOutput())
                .put("NewLineSplitter", buildTokenSplitter("\r\n"));
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                socket.write("Hello \r\nWorld");
                socket.handler(buffer -> {
                    context.assertEquals("Hello ", buffer.toString());


                    async.complete();

                });
            });

        }));
        vertx.setTimer(5000, event -> {
            context.fail("timed out");
        });
    }

    @Test
    public void testCanEchoPacketStream(TestContext context) {
        final DeploymentOptions options = new DeploymentOptions();
        JsonObject inputConfig = buildInput();
        inputConfig.put("splitter", "PacketSplitter");
        final JsonObject config = new JsonObject();
        config
                .put("usher", new JsonObject().put("pipelineInstances", 1))
                .put("PayloadEncoder", new JsonObject())
                .put("Router", inputConfig)
                .put("EchoBackend", buildOutput())
                .put("PacketSplitter", buildPacketSplitter());
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                final Buffer fakeStartupPacket = createFakeStartupPacket();
                socket.write(fakeStartupPacket);
                socket.handler(buffer -> {
                    context.assertEquals(fakeStartupPacket.length(), buffer.length());


                    async.complete();

                });
            });

            vertx.setTimer(5000, event -> {
                context.fail("timed out");
            });

        }));
    }

    @Test
    public void testInvalidPacketShouldError(TestContext context) {
        final DeploymentOptions options = new DeploymentOptions();
        JsonObject inputConfig = buildInput();
        inputConfig.put("splitter", "PacketSplitter");
        final JsonObject config = new JsonObject();
        config
                .put("usher", new JsonObject().put("pipelineInstances", 1))
                .put("PayloadEncoder", new JsonObject())
                .put("Router", inputConfig)
                .put("EchoBackend", buildOutput())
                .put("PacketSplitter", buildPacketSplitter());
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                final Buffer bogusPacket = Buffer.buffer("bogus");
                socket.write(bogusPacket);
                socket.handler(buffer -> {
                    final short response = buffer.getShort(0);
                    context.assertEquals((short)0xff00, Short.reverseBytes(response));

                });

                // Expect the server to close the socket on us
                socket.closeHandler(v->{
                    async.complete();
                });
            });

            vertx.setTimer(5000, event -> {
                context.fail("timed out");
            });

        }));
    }

    protected Buffer createFakeStartupPacket() {
        final Buffer fakeStartupPacket = Buffer.buffer();
        fakeStartupPacket.appendByte((byte) 0x03);
        fakeStartupPacket.appendByte((byte) 0x03);
        fakeStartupPacket.appendBytes(new byte[26]);
        return fakeStartupPacket;
    }

    private JsonObject buildPacketSplitter() {
        return new JsonObject().put("type", "PacketSplitter").put("rules",
                new JsonObject("{\n" +
                        "               \"type\": \"fixed\",\n" +
                        "               \"length\": 2,\n" +
                        "               \"nextRule\": {\n" +
                        "                   \"type\": \"typeMap\",\n" +
                        "                   \"typeMap\": [\n" +
                        "                       {\n" +
                        "                           \"bytes\": [\"1\", \"1\"],\n" +
                        "                           \"length\": 28,\n" +
                        "                           \"nextRule\": {\n" +
                        "                               \"type\": \"dynamicCount\",\n" +
                        "                               \"counterPosition\": 26,\n" +
                        "                               \"lengthPerSegment\": 4\n" +
                        "                           }\n" +
                        "                       },\n" +
                        "                       {\n" +
                        "                           \"bytes\": [\"2\", \"2\"],\n" +
                        "                           \"length\": 24,\n" +
                        "                           \"nextRule\": {\n" +
                        "                               \"type\": \"dynamicCount\",\n" +
                        "                               \"counterPosition\": 32,\n" +
                        "                               \"lengthPerSegment\": 4\n" +
                        "                           }\n" +
                        "                       },\n" +
                        "                       {\n" +
                        "                           \"bytes\": [\"3\", \"3\"],\n" +
                        "                           \"length\": 26\n" +
                        "                       },\n" +
                        "                       {\n" +
                        "                           \"bytes\": [\"4\", \"4\"],\n" +
                        "                           \"length\": 23\n" +
                        "                       },\n" +
                        "                       {\n" +
                        "                           \"bytes\": [\"7\", \"7\"],\n" +
                        "                           \"length\": 16,\n" +
                        "                           \"nextRule\": {\n" +
                        "                               \"type\": \"dynamicCount\",\n" +
                        "                               \"counterPosition\": 14,\n" +
                        "                               \"lengthPerSegment\": 7\n" +
                        "                           }\n" +
                        "                       }\n" +
                        "                   ]\n" +
                        "               }\n" +
                        "           }"));
    }

    private JsonObject buildOutput() {
        return new JsonObject().put("type", "TcpOutput").put("host", "localhost").put("port", 9193).put("encoder", "PayloadEncoder").put("messageMatcher", "#{pack.message.localPort == 2500}");
    }

    private JsonObject buildTokenSplitter(String delimiter) {
        return new JsonObject().put("type", "TokenSplitter").put("delimiter", delimiter);
    }

    private JsonObject buildInput() {
        return new JsonObject().put("type", "TcpInput").put("host", "localhost").put("port", 2500).put("encoder", "PayloadEncoder");
    }


}
