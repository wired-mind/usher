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

    Vertx vertx;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx(new VertxOptions());
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
