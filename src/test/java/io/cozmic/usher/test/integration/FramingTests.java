package io.cozmic.usher.test.integration;


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
        final JsonObject output = buildOutput();
        output.put("useFraming", true);
        output.put("splitter", "UsherV1FramingSplitter");
        config
                .put("PayloadEncoder", new JsonObject())
                .put("Router", buildInput())
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

    private JsonObject buildOutput() {
        return new JsonObject().put("type", "TcpOutput").put("host", "localhost").put("port", 9193).put("encoder", "PayloadEncoder").put("messageMatcher", "#{localPort == 2500}");
    }

    private JsonObject buildInput() {
        return new JsonObject().put("type", "TcpInput").put("host", "localhost").put("port", 2500).put("encoder", "PayloadEncoder");
    }




}