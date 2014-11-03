package io.cozmic.usher.test.integration;

import io.cozmic.usher.TimeoutLogger;
import org.junit.Test;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Created by chuck on 9/25/14.
 */
public class NetProxyIntegrationTests extends NetProxyBaseIntegrationTests {




    @Test
    public void testSimpleProxyRequest() throws InterruptedException {


            sendToProxy(createFakeTrackingPacket(), 5, 500000, new Handler<Buffer>() {
                @Override
                public void handle(Buffer response) {
                    final byte type = response.getByte(0);
                    assertEquals(0x11, type);

                }
            }, new Handler<Void>() {

                @Override
                public void handle(Void event) {

                    vertx.eventBus().send(TimeoutLogger.COUNT_ADDRESS, true, new Handler<Message<Integer>>() {
                                @Override
                                public void handle(Message<Integer> event) {

                                    container.logger().info("Timeout log count is " + event.body());
                                    testComplete();
                                }
                            });


                }
            });

        vertx.setPeriodic(5000, new Handler<Long>() {
            @Override
            public void handle(Long event) {
                vertx.eventBus().send(TimeoutLogger.COUNT_ADDRESS, true, new Handler<Message<Integer>>() {
                    @Override
                    public void handle(Message<Integer> event) {

                        container.logger().info("Timeout log count is " + event.body());

                    }
                });
            }
        });
        }

    }
