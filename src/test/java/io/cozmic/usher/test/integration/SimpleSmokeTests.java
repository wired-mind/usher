package io.cozmic.usher.test.integration;


import io.cozmic.usher.RawEchoChamber;
import io.cozmic.usher.Start;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
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
public class SimpleSmokeTests {

    Vertx vertx;
    private RawEchoChamber rawEchoChamber;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();
        rawEchoChamber = new RawEchoChamber();
        vertx.deployVerticle(rawEchoChamber, context.asyncAssertSuccess());
    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }


    @Test
    public void testCanStart(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions("/config_simple_echo.json");
        final Async async = context.async();
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final HttpClient httpClient = vertx.createHttpClient();
            httpClient.getNow(8080, "localhost", "/", response -> {
                context.assertEquals(200, response.statusCode());
                vertx.undeploy(deploymentID, context.asyncAssertSuccess());
                async.complete();
            });

        }));

    }

    @Test
    public void testCanEcho(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions("/config_simple_echo.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                socket.handler(buffer -> {
                    context.assertEquals("Hello", buffer.toString());
                    async.complete();
                });
                socket.write("Hello");
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
    public void muxShouldKeepBackendSocketOpen(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions("/config_simple_echo.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                socket.handler(buffer -> {
                    context.assertEquals("Hello", buffer.toString());
                    socket.close();
                });

                // One second after we disconnect, verify the proper server behavior
                socket.closeHandler(v -> {
                    vertx.setTimer(1000, timerId -> {
                        context.assertEquals(1, rawEchoChamber.getConnectionCount(), "After closing a socket the backend socket should still be open under normal circumstances. See minIdle documentation on ObjectPool for different scenarios.");
                        async.complete();
                    });
                });

                socket.write("Hello");
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
    public void muxShouldCloseBackendSocketWhenObjectPoolingIsDisabled(TestContext context) {
        final DeploymentOptions options = buildDeploymentOptions("/config_simple_echo_no_pooling.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                socket.handler(buffer -> {
                    context.assertEquals("Hello", buffer.toString());
                    socket.close();
                });

                // One second after we disconnect, verify the proper server behavior
                socket.closeHandler(v -> {
                    vertx.setTimer(1000, timerId -> {
                        context.assertEquals(0, rawEchoChamber.getConnectionCount(), "After closing a socket the backend socket should be closed because object pooling is disabled. See minIdle documentation on ObjectPool for different scenarios.");
                        async.complete();
                    });
                });

                socket.write("Hello");
            });

            vertx.setTimer(5000, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    context.fail("timed out");
                }
            });

        }));
    }


    public DeploymentOptions buildDeploymentOptions(String configFile) {
        JsonObject config = null;
        try {
            final URI uri = getClass().getResource(configFile).toURI();
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
