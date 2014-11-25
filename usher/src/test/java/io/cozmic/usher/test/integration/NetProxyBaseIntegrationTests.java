package io.cozmic.usher.test.integration;

import io.cozmic.usher.Start;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.concurrent.atomic.AtomicInteger;

import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Created by chuck on 9/26/14.
 */
public abstract class NetProxyBaseIntegrationTests extends ProxyBaseIntegrationTests {

    private AtomicInteger counter = new AtomicInteger();


    protected String getProxyName() {
        return Start.class.getName();
    }

    protected String getFakeName() {
        return FakeService.class.getName();
    }

    @Override
    protected JsonObject getProxyConfig() {
        final JsonObject config = new JsonObject();
        config.putString("serviceHost", FakeService.FAKE_SERVICE_HOST);
        config.putNumber("servicePort", FakeService.FAKE_SERVICE_PORT);
        return new JsonObject().putObject("proxyConfig", config);
    }

    @Override
    protected JsonObject getFakeConfig() {
        return new JsonObject();
    }

    protected void sendToProxy(final Buffer buffer, final Handler<Buffer> responseHandler) {
        sendToProxy(buffer, 1, 1, responseHandler, new Handler<Void>() {
            @Override
            public void handle(Void event) {
                testComplete();
            }
        });
    }

    protected void sendToProxy(final Buffer buffer, final Handler<Buffer> responseHandler, final Handler<Void> doneHandler) {
        sendToProxy(buffer, 1, 1, responseHandler, doneHandler);
    }
    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
    protected void sendToProxy(final Buffer buffer, final int botCount, final int totalRequests, final Handler<Buffer> responseHandler, final Handler<Void> doneHandler) {


        final int repeat = totalRequests / botCount;
        final int actualTotalRequests = repeat * botCount;
        final JsonObject config = new JsonObject();
        config.putNumber("repeat", repeat);
        config.putBinary("buffer", buffer.getBytes());
        container.deployVerticle(CozmicBot.class.getName(), config, botCount, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> event) {
                if (event.failed()) {
                    container.logger().error(event.cause().getMessage());
                    fail();
                    return;
                }
                container.logger().info(botCount + " bots deployed");
            }
        });

        vertx.eventBus().registerLocalHandler(CozmicBot.COZMICBOT_RESULT, new Handler<Message<Buffer>>() {
            @Override
            public void handle(Message<Buffer> message) {
                final int count = counter.incrementAndGet();
                if (count == actualTotalRequests) {
                    doneHandler.handle(null);
                    return;
                }

                responseHandler.handle(message.body());
            }
        });


    }


    protected Buffer createFakeStartupPacket() {
        final Buffer fakeStartupPacket = new Buffer();
        fakeStartupPacket.appendByte((byte) 0x03);
        fakeStartupPacket.appendByte((byte) 0x03);
        fakeStartupPacket.appendBytes(new byte[26]);
        return fakeStartupPacket;
    }
}
