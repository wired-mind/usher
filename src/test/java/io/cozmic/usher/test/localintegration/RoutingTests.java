package io.cozmic.usher.test.localintegration;


import io.cozmic.usher.Start;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

/**
 * Created by chuck on 6/29/15.
 */
@RunWith(VertxUnitRunner.class)
public class RoutingTests {

    Vertx vertx;
    private FakeService fooService;
    private FakeService barService;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();
        fooService = new FakeService(Buffer.buffer("foo"), 9192);
        barService = new FakeService(Buffer.buffer("bar"), 9193);
        vertx.deployVerticle(fooService, context.asyncAssertSuccess(r -> vertx.deployVerticle(barService, context.asyncAssertSuccess())));
    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }


    /**
     * Two different Inputs with different ports each route to a different backout Output
     * @param context
     */
    @Test
    public void testMessageMatchRouting(TestContext context) {
        final DeploymentOptions options = new DeploymentOptions();

        final JsonObject config = new JsonObject();


        config
                .put("PayloadEncoder", new JsonObject())
                .put("FooRouter", buildFooInput())
                .put("BarRouter", buildBarInput())
                .put("FooBackend", buildFooOutput(2500))
                .put("BarBackend", buildBarOutput(2501));
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            final NetClient netClient = vertx.createNetClient();
            netClient.connect(2500, "localhost", context.asyncAssertSuccess(fooSocket -> {
                fooSocket.handler(fooBuffer -> {
                    context.assertEquals("foo", fooBuffer.toString());
                    netClient.connect(2501, "localhost", context.asyncAssertSuccess(barSocket -> {
                        barSocket.handler(barBuffer -> {
                            context.assertEquals("bar", barBuffer.toString());
                            async.complete();
                        });
                        barSocket.write("Hello Bar");
                    }));


                });

                fooSocket.write("Hello Foo");
            }));
            vertx.setTimer(5000, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    context.fail("timed out");
                }
            });
        }));
    }

    /**
     * One Input clones a message to two backends. Only the first backend will respond.
     * @param context
     */
    @Test
    public void testMessageMatchCloning(TestContext context) {
        final DeploymentOptions options = new DeploymentOptions();

        final JsonObject config = new JsonObject();


        config
                .put("PayloadEncoder", new JsonObject())
                .put("FooBarRouter", buildFooBarInput())
                .put("FooBackend", buildFooOutput(2500))
                .put("BarBackend", buildBarOutput(2500));
        options.setConfig(config);
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            final NetClient netClient = vertx.createNetClient();
            netClient.connect(2500, "localhost", context.asyncAssertSuccess(fooBarSocket -> {
                final String payload = "Hello Foo and Bar";
                AtomicInteger responseCount = new AtomicInteger();
                fooBarSocket.handler(fooBuffer -> {

                    final String response = fooBuffer.toString();
                    int responses = 0;
                    if (Objects.equals(response, "foo")) {
                        context.assertEquals(payload, fooService.getLastBuffer().toString());
                        responses = responseCount.incrementAndGet();
                    }
                    if (Objects.equals(response, "bar")) {
                        context.assertEquals(payload, barService.getLastBuffer().toString());
                        responses = responseCount.incrementAndGet();
                    }
                    if (responses == 2) {
                        async.complete();
                    }
                });

                fooBarSocket.write(payload);
            }));
            vertx.setTimer(5000, event -> context.fail("timed out"));
        }));
    }

    private JsonObject buildFooBarInput() {
        return new JsonObject().put("type", "TcpInput").put("host", "localhost").put("port", 2500).put("encoder", "PayloadEncoder");
    }

    private JsonObject buildBarInput() {
        return new JsonObject().put("type", "TcpInput").put("host", "localhost").put("port", 2501).put("encoder", "PayloadEncoder");
    }




    private JsonObject buildFooOutput(int inputFilterPort) {
        return new JsonObject().put("type", "TcpOutput").put("host", "localhost").put("port", 9192).put("encoder", "PayloadEncoder").put("messageMatcher", String.format("#{pack.message.localPort == %s}", inputFilterPort));
    }
    private JsonObject buildBarOutput(int inputFilterPort) {
        return new JsonObject().put("type", "TcpOutput").put("host", "localhost").put("port", 9193).put("encoder", "PayloadEncoder").put("messageMatcher", String.format("#{pack.message.localPort == %s}", inputFilterPort));
    }


    private JsonObject buildFooInput() {
        return new JsonObject().put("type", "TcpInput").put("host", "localhost").put("port", 2500).put("encoder", "PayloadEncoder");
    }




}
