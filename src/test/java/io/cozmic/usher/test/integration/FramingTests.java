package io.cozmic.usher.test.integration;


import io.cozmic.usher.RawEchoChamber;
import io.cozmic.usher.Start;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

/**
 * Created by chuck on 6/29/15.
 */
@RunWith(VertxUnitRunner.class)
public class FramingTests {

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
    public void testCanFrameOutputAndSplitEchoResponse(TestContext context) {
        final DeploymentOptions options = new DeploymentOptions();

        final JsonObject config = new JsonObject();
        final JsonObject output = buildOutput("PayloadEncoder", "NullDecoder");
        output.put("useFraming", true);
        output.put("splitter", "UsherV1FramingSplitter");

        final JsonObject input = buildInput("PayloadEncoder", "NullDecoder");
        config
                .put("UsherV1FramingSplitter", new JsonObject().put("useMessageBytes", false))
                .put("PayloadEncoder", new JsonObject())
                .put("Router", input)
                .put("EchoBackend", output);
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                socket.write("Hello World");
                socket.handler(buffer -> {
                    context.assertEquals("Hello World", buffer.toString());

                    async.complete();

                });
            });
            vertx.setTimer(5000, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    context.fail("timed out");
                }
            });
        }));
    }

    @Test
    public void testCanFrameWithJsonEncoding(TestContext context) {
        final DeploymentOptions options = new DeploymentOptions();

        final JsonObject config = new JsonObject();
        final JsonObject output = buildOutput("JsonEncoder", "JsonDecoder");
        output.put("useFraming", true);
        output.put("splitter", "UsherV1FramingSplitter");
        config
                .put("UsherV1FramingSplitter", new JsonObject().put("useMessageBytes", false))
                .put("PayloadEncoder", new JsonObject())
                .put("JsonEncoder", buildJsonEncoder())
                .put("JsonDecoder", buildJsonDecoder())
                .put("Router", buildInput("JsonEncoder", "JsonDecoder"))
                .put("EchoBackend", output);
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                socket.write("{}");
                socket.handler(buffer -> {
                    context.assertEquals("{}", buffer.toString());

                    async.complete();

                });
            });
            vertx.setTimer(5000, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    context.fail("timed out");
                }
            });
        }));
    }

    private JsonObject buildOutput(String encoder, String decoder) {
        return new JsonObject().put("type", "TcpOutput").put("host", "localhost").put("port", 9193).put("encoder", encoder).put("decoder", decoder).put("messageMatcher", "#{1==1}");
    }

    private JsonObject buildInput(String encoder, String decoder) {
        return new JsonObject().put("type", "TcpInput").put("host", "localhost").put("port", 2500).put("encoder", encoder).put("decoder", decoder);
    }

    private JsonObject buildJsonEncoder() {
        return new JsonObject().put("type", "JsonEncoder");
    }

    private JsonObject buildJsonDecoder() {
        return new JsonObject().put("type", "JsonDecoder");
    }

}
