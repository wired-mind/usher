package io.cozmic.usher.test.localintegration;


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
public class ConnectionTimingTests {

    Vertx vertx;
    private RawEchoChamber rawEchoChamber;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();

    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }



    /**
     * The purpose of this test is isolate a intermittent failure. When pooling is off
     * sometimes the socket reads data before the Channel is fully setup. When the bug is fixed,
     * this should never fail. We can reproduce the conditions by simulating a slow start of the backend
     * echo chamber. Basically while the mux waits for the connection to establish, the client has already
     * sent data. We need to make sure this data doesn't get lost.
     * @param context
     */
    @Test
    public void testCanEchoWithSlowBackendAndNoPooling(TestContext context) {
        vertx.setTimer(1000, timerId -> {
            rawEchoChamber = new RawEchoChamber();
            vertx.deployVerticle(rawEchoChamber, context.asyncAssertSuccess());
        });

        final DeploymentOptions options = buildDeploymentOptions("/config_simple_echo_no_pooling.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                socket.write("Hello");
                socket.handler(buffer -> {
                    context.assertEquals("Hello", buffer.toString());
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
