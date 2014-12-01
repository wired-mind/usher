package io.cozmic.usher.test.integration;

import io.cozmic.usher.Start;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.fail;

/**
 * Created by chuck on 9/26/14.
 */
public abstract class ProxyBaseIntegrationTests extends TestVerticle {

    protected abstract String getFakeName();

    protected abstract JsonObject getFakeConfig();

    @Override
    public void start() {
        initialize();


        container.deployVerticle(getFakeName(), getFakeConfig(), 2, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> event) {
                if (event.failed()) {
                    container.logger().error(event.cause());
                }
                assertTrue(event.succeeded());
                assertNotNull("deploymentID should not be null", event.result());

                assertTrue(event.succeeded());
                assertNotNull("deploymentID should not be null", event.result());


                JsonObject config = null;
                try {
                    final URI uri = getClass().getResource("/config.json").toURI();
                    final String configString = new String(Files.readAllBytes(Paths.get(uri)));
                    config = new JsonObject(configString);
                } catch (URISyntaxException | IOException e) {
                    fail(e.getMessage());
                }

                makeDbPathRandom(config, "journalerConfig");
                makeDbPathRandom(config, "timeoutLogConfig");
                makeDbPathRandom(config, "connectionLogConfig");

                container.deployVerticle(Start.class.getName(), config, 1, new Handler<AsyncResult<String>>() {
                    @Override
                    public void handle(AsyncResult<String> event) {
                        if (event.failed()) {
                            final Throwable cause = event.cause();
                            container.logger().error(cause.getMessage(), cause);
                        }

                        assertTrue(event.succeeded());
                        assertNotNull("deploymentID should not be null", event.result());

                        startTests();
                    }
                });


            }
        });
    }

    private void makeDbPathRandom(JsonObject config, String dbName) {
        final JsonObject persistence = config.getObject("persistence");
        final JsonObject conf = persistence.getObject(dbName);
        conf.putString("dbPath", UUID.randomUUID().toString());
    }
}
