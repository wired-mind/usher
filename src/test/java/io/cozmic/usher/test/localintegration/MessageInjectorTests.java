package io.cozmic.usher.test.localintegration;

import io.cozmic.usher.RawEchoChamber;
import io.cozmic.usher.Start;
import io.cozmic.usher.test.integration.EventBusFilter;
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
 * Created by chuck on 9/30/15.
 */
@RunWith(VertxUnitRunner.class)
public class MessageInjectorTests {

    private Vertx vertx;

    @Before
    public void before(TestContext context) {
        vertx = Vertx.vertx();
    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testCanInjectToEventBusFilter(TestContext context) {
        final String expectedMessage = "Hello";
        final DeploymentOptions options = buildDeploymentOptions("/config_message_injector.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();
                vertx.eventBus().<Integer>consumer(EventBusFilter.EVENT_BUS_ADDRESS, msg -> {
                    final Integer hashCode = msg.body();
                    context.assertEquals(expectedMessage.hashCode(), hashCode);
                    async.complete();
                });

                socket.write(expectedMessage);
            });

            vertx.setTimer(5000, event -> context.fail("timed out"));

        }));
    }

    @Test
    public void testCannotInjectToEventBusFilterIfMessageMatcherMatchesOnInjector(TestContext context) {
        final String expectedMessage = "MatchBoth";
        final DeploymentOptions options = buildDeploymentOptions("/config_message_injector.json");
        vertx.deployVerticle(Start.class.getName(), options, context.asyncAssertSuccess(deploymentID -> {
            final Async async = context.async();
            vertx.createNetClient().connect(2500, "localhost", asyncResult -> {
                final NetSocket socket = asyncResult.result();

                socket.closeHandler(v->{
                    async.complete();
                });
                socket.write(expectedMessage);
            });

            vertx.setTimer(5000, event -> context.fail("timed out"));

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
