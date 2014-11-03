package io.cozmic.usher.test.integration;

import io.cozmic.usherprotocols.core.Counter;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.shareddata.ConcurrentSharedMap;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.assertTrue;

/**
 * Created by chuck on 9/26/14.
 */
public abstract class ProxyBaseIntegrationTests extends TestVerticle {

    protected abstract String getProxyName();

    protected abstract String getFakeName();

    protected abstract JsonObject getProxyConfig();

    protected abstract JsonObject getFakeConfig();

    @Override
    public void start() {
        initialize();

        final ConcurrentSharedMap<String, Counter> counters = vertx.sharedData().getMap("counters");
        counters.put("fakesend", new Counter());
        counters.put("fakereceive", new Counter("fakereceive"));
        counters.put("purged_fake", new Counter("purged_fake"));
        counters.put("send", new Counter());
        counters.put("receive", new Counter("receive"));
        counters.put("purged_proxy", new Counter("purged_proxy"));
        counters.put("received_bytes", new Counter("received_bytes"));

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


                container.deployVerticle(getProxyName(), getProxyConfig(), 1, new Handler<AsyncResult<String>>() {
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
}
