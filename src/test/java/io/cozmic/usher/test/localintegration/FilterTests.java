package io.cozmic.usher.test.localintegration;


import io.cozmic.usher.Start;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
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
public class FilterTests {

    public static final int MAX_RETRIES_FOR_TESTS = 4;
    Vertx vertx;
    private JsonObject ignoreErrorStrategy = new JsonObject().put("type", "ignore");
    private JsonObject retryErrorStrategy = new JsonObject().put("type", "retry").put("maxRetries", MAX_RETRIES_FOR_TESTS);

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();
    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }



    @Test
    public void testFilterCanEcho(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions("/config_filter_echo.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                socket.write("Hello Filter");
                socket.handler(buffer -> {
                    context.assertEquals("Hello Filter", buffer.toString());
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
    public void testFilterWithExceptionShouldCloseSocket(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions("/config_filter_w_err.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                final Buffer bogusPacket = Buffer.buffer("bogus");
                socket.write(bogusPacket);
                // Expect the server to close the socket on us
                socket.closeHandler(v -> {
                    async.complete();
                });
            });

            vertx.setTimer(5000, event -> {
                context.fail("timed out");
            });

        }));
    }

    /**
     * Explicitly set the error strategy to ignore and ensure that despite an exception in the filter, the writeComplete
     * still succeeds.
     * @param context
     */
    @Test
    public void testFilterWithExceptionAndIgnoreErrorStrategyShouldComplete(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions("/config_filter_w_err.json");
        options.getConfig().getJsonObject("ErrorBackend").put("errorStrategy", ignoreErrorStrategy);
        final Async async = context.async();
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            vertx.eventBus().consumer("fake.complete", message -> {
                context.assertEquals("Ok", message.body().toString());
                async.complete();
            });
            vertx.eventBus().publish("fake.input", Buffer.buffer("Hello"));


            vertx.setTimer(5000, event -> {
                context.fail("timed out");
            });

        }));
    }

    /**
     * Simulate a timeout in the filter. The ignore strategy will still result in success.
     * @param context
     */
    @Test
    public void testFilterThatTimesOutWithIgnoreErrorStrategyShouldComplete(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions("/config_filter_w_err.json");
        final JsonObject errorBackend = options.getConfig().getJsonObject("ErrorBackend");
        errorBackend.put("repeatErrorCount", 1); //let's simulate a single timeout
        final int timeout = 4_000;
        errorBackend.put("timeout", timeout);
        final int delay = timeout + 1_000;
        errorBackend.put("delay", delay); //simulate a delay greater than the timeout value.
        errorBackend.put("errorStrategy", ignoreErrorStrategy);
        final Async async = context.async();
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            vertx.eventBus().consumer("fake.complete", message -> {
                context.assertEquals("Ok", message.body().toString());
                async.complete();
            });
            vertx.eventBus().publish("fake.input", Buffer.buffer("Hello"));


            //Fail the test exactly before the delay. If timeout works successfully, it should trigger before this timer.
            vertx.setTimer(delay-1, event -> {
                context.fail("timed out");
            });

        }));
    }

    @Test
    public void testFilterWithExceptionAndRetryErrorStrategyShouldFailAfterExhaustingAttempts(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions("/config_filter_w_err.json");
        options.getConfig().getJsonObject("ErrorBackend").put("errorStrategy", retryErrorStrategy);
        final Async async = context.async();
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            vertx.eventBus().consumer("fake.close", message -> {
                context.assertEquals("Closed", message.body().toString());
                async.complete();
            });
            vertx.eventBus().publish("fake.input", Buffer.buffer("Hello"));


            vertx.setTimer(15_000, event -> {
                context.fail("timed out");
            });

        }));
    }

    @Test
    public void testFilterRetryErrorStrategyEventuallyWillSucceed(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions("/config_filter_w_err.json");
        final JsonObject errorBackend = options.getConfig().getJsonObject("ErrorBackend");
        errorBackend.put("repeatErrorCount", MAX_RETRIES_FOR_TESTS - 1);
        errorBackend.put("errorStrategy", retryErrorStrategy);
        final Async async = context.async();
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            vertx.eventBus().consumer("fake.complete", message -> {
                context.assertEquals("Ok", message.body().toString());
                async.complete();
            });
            vertx.eventBus().publish("fake.input", Buffer.buffer("Hello"));


            vertx.setTimer(15_000, event -> {
                context.fail("timed out");
            });

        }));
    }

    @Test
    public void testFilterTimeoutWillCauseRetry(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions("/config_filter_w_err.json");
        final JsonObject errorBackend = options.getConfig().getJsonObject("ErrorBackend");
        errorBackend.put("repeatErrorCount", 1); //let's simulate a single timeout
        errorBackend.put("delay", 5_000);
        errorBackend.put("errorStrategy", retryErrorStrategy);
        final Async async = context.async();
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            vertx.eventBus().consumer("fake.complete", message -> {
                context.assertEquals("Ok", message.body().toString());
                async.complete();
            });
            vertx.eventBus().publish("fake.input", Buffer.buffer("Hello"));


            vertx.setTimer(15_000, event -> {
                context.fail("timed out");
            });

        }));
    }


    public DeploymentOptions buildDeploymentOptions(String configFIle) {
        JsonObject config = null;
        try {
            final URI uri = getClass().getResource(configFIle).toURI();
            final String configString = new String(Files.readAllBytes(Paths.get(uri)));
            config = new JsonObject(configString);
        } catch (URISyntaxException | IOException e) {
            fail(e.getMessage());
        }
        final DeploymentOptions options = new DeploymentOptions();
        options.setConfig(config);
        return options;
    }


}
