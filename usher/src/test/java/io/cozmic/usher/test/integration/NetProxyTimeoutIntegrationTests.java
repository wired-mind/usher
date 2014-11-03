package io.cozmic.usher.test.integration;

import io.cozmic.usher.core.ProxyTunnel;
import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.json.JsonObject;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Created by chuck on 9/26/14.
 */
public class NetProxyTimeoutIntegrationTests extends NetProxyBaseIntegrationTests {

    @Override
    protected JsonObject getFakeConfig() {
        final JsonObject config = super.getFakeConfig();
        config.putNumber("delay", ProxyTunnel.DEFAULT_TIMEOUT + 100);
        return config;
    }

    @Test
    public void testSimpleProxyRequest() {

        sendToProxy(createFakeTrackingPacket(), new Handler<Buffer>() {
            @Override
            public void handle(Buffer response) {
                final int type = response.getInt(0);
                assertEquals(type, 99);
                testComplete();
            }
        });
    }

}
