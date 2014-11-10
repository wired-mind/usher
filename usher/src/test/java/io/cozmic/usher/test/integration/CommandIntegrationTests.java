package io.cozmic.usher.test.integration;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Created by chuck on 11/10/14.
 */
public class CommandIntegrationTests extends NetProxyBaseIntegrationTests {

    @Test
    public void testTimeoutsList()  {

        String first = "aaa";
        String second = "bbb";
        final byte[] firstBytes = first.getBytes();
        final byte[] secondBytes = second.getBytes();

        final byte[] allBytes = ArrayUtils.addAll(firstBytes, secondBytes);
        final byte[] header = {0x01};
        final byte[] envelopeBytes = ArrayUtils.addAll(header, allBytes);
        String all = new String(allBytes);
        String envelope = new String(envelopeBytes);
        vertx.createNetClient().connect(2001, new Handler<AsyncResult<NetSocket>>() {
            @Override
            public void handle(AsyncResult<NetSocket> asyncResult) {
                if (asyncResult.failed()) {
                    container.logger().error(asyncResult.cause().getMessage());
                    fail();
                    return;
                }

                final NetSocket socket = asyncResult.result();
                socket.write("timeouts list\n");
                socket.dataHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer event) {

                        //Lame test right now
                        testComplete();
                    }
                });
            }
        });


    }
}