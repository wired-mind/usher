package io.cozmic.usher.test.integration;

import com.google.common.primitives.UnsignedBytes;
import io.cozmic.usher.PersistenceVerticle;
import io.cozmic.usherprotocols.core.Request;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Created by chuck on 11/10/14.
 */
public class CommandIntegrationTests extends NetProxyBaseIntegrationTests {

    @Test
    public void testTimeoutsList() {
        final Request request = new Request("test", "test", 1, new Buffer("We can send anything in for a test"));
        vertx.eventBus().send(PersistenceVerticle.TIMEOUT_LOG_ADDRESS, request.buildEnvelope());
        vertx.eventBus().send(PersistenceVerticle.TIMEOUT_LOG_ADDRESS, request.buildEnvelope());


        vertx.createNetClient().connect(2001, new Handler<AsyncResult<NetSocket>>() {
            @Override
            public void handle(AsyncResult<NetSocket> asyncResult) {
                if (asyncResult.failed()) {
                    container.logger().error(asyncResult.cause().getMessage());
                    fail();
                    return;
                }

                final AtomicInteger counter = new AtomicInteger();
                final NetSocket socket = asyncResult.result();
                socket.write("timeouts list\n");
                socket.dataHandler(RecordParser.newDelimited("\n", new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer event) {
                        container.logger().info(event.toString());
                        final int count = counter.incrementAndGet();
                        if (count == 2) {
                            testComplete();
                        }
                    }
                }));
            }
        });


    }



    @Test
    public void testTimeoutsReplay() {
        final Request request = new Request("test", "test", 1, new Buffer("We can send anything in for a test"));
        vertx.eventBus().send(PersistenceVerticle.TIMEOUT_LOG_ADDRESS, request.buildEnvelope());
        vertx.eventBus().send(PersistenceVerticle.TIMEOUT_LOG_ADDRESS, request.buildEnvelope());


        vertx.createNetClient().connect(2001, new Handler<AsyncResult<NetSocket>>() {
            @Override
            public void handle(AsyncResult<NetSocket> asyncResult) {
                if (asyncResult.failed()) {
                    container.logger().error(asyncResult.cause().getMessage());
                    fail();
                    return;
                }


                final NetSocket socket = asyncResult.result();
                socket.write("timeouts replay\n");
                socket.dataHandler(RecordParser.newDelimited("\n", new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer event) {
                        final String message = event.toString();
                        if (message.equals("ok - null")) {
                            String reply = UUID.randomUUID().toString();
                            vertx.eventBus().registerHandler(reply, new Handler<Message<Long>>() {
                                @Override
                                public void handle(Message<Long> event) {
                                    assertEquals(0l, event.body().longValue());
                                    testComplete();
                                }
                            });
                            vertx.eventBus().send(PersistenceVerticle.TIMEOUT_LOG_COUNT_ADDRESS, reply);

                        }

                    }
                }));
            }
        });


    }
}