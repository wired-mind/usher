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
 * These tests are kind of stand in for what it would be like to have Pojo's or other more advanced encoders/decoders
 */
@RunWith(VertxUnitRunner.class)
public class JsonObjectTests {

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
    public void testCanDoSimpleJsonObjectFilter(TestContext context) {
        final DeploymentOptions options = new DeploymentOptions();

        final JsonObject config = new JsonObject();
        final JsonObject jsonFilter = buildJsonFilter();

        config
                .put("Router", buildInput())
                .put("HelloWorldJsonFilter", jsonFilter)
                .put("JsonEncoder", buildJsonEncoder())
                .put("JsonDecoder", buildJsonDecoder());
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                socket.write("{}");
                socket.handler(buffer -> {

                    context.assertEquals("{\"hello\":\"world\",\"original\":{}}", buffer.toString());

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

    private JsonObject buildJsonEncoder() {
        return new JsonObject().put("type", "JsonEncoder");
    }

    private JsonObject buildJsonDecoder() {
        return new JsonObject().put("type", "JsonDecoder");
    }

    private JsonObject buildJsonFilter() {
        return new JsonObject().put("type", "io.cozmic.usher.test.integration.HelloWorldJsonFilter").put("messageMatcher", "#{1==1}");
    }

    private JsonObject buildInput() {
        return new JsonObject().put("type", "TcpInput").put("host", "localhost").put("port", 2500).put("decoder", "JsonDecoder").put("encoder", "JsonEncoder");
    }



}
