package io.cozmic.usher.test.integration;

import io.cozmic.usher.NetProxy;
import io.cozmic.usher.Proxy;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;
import org.vertx.java.core.streams.Pump;
import org.vertx.java.core.streams.ReadStream;

import java.util.concurrent.atomic.AtomicInteger;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.fail;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Created by chuck on 9/26/14.
 */
public abstract class NetProxyBaseIntegrationTests extends ProxyBaseIntegrationTests {
    private AtomicInteger counter = new AtomicInteger();
    private AtomicInteger sendCounter = new AtomicInteger();
    private NetClient netClient;

    protected String getProxyName() {
        return NetProxy.class.getName();
    }

    protected String getFakeName() {
        return FakeService.class.getName();
    }

    @Override
    protected JsonObject getProxyConfig() {
        final JsonObject config = new JsonObject();
        config.putString("service_host", FakeService.FAKE_SERVICE_HOST);
        config.putNumber("service_port", FakeService.FAKE_SERVICE_PORT);
        return config;
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
        netClient = vertx.createNetClient().setConnectTimeout(5000).setReconnectAttempts(100).setReconnectInterval(1000);

        final int repeat = totalRequests / botCount;
        final int actualTotalRequests = repeat * botCount;
        container.deployVerticle(CozmicBot.class.getName(), botCount, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> event) {
                container.logger().info("Bots deployed");
            }
        });

        for (int i = 0; i < botCount; i++) {
            netClient.connect(Proxy.DEFAULT_PORT, Proxy.DEFAULT_HOST, new Handler<AsyncResult<NetSocket>>() {
                @Override
                public void handle(AsyncResult<NetSocket> event) {
                    if (event.failed()) {
                        container.logger().error(event.cause().getMessage());
                        fail();
                    }
                    //container.logger().info("Test: Socket to proxy established");
                    final NetSocket socket = event.result();


                    final RecordParser responseParser = RecordParser.newFixed(2, new Handler<Buffer>() {
                        int socketReceiveCount = 0;
                        @Override
                        public void handle(Buffer event) {
                            final int count = counter.incrementAndGet();

                            socketReceiveCount++;
                            if (socketReceiveCount == repeat) {
                               // getContainer().logger().info("All done with this socket. Received" + socketReceiveCount);
                                socket.dataHandler(null);
                                socket.close();
                            }
//                            if (count % 10000 == 0) {
//
//                                getContainer().logger().info("Sent/received (" + bytesToHex(event.getBytes()) + "): + " + count);
//                            }
                           // getContainer().logger().info("Sent/received (" + bytesToHex(event.getBytes()) + "): + " + count);
                            if (count == actualTotalRequests) {
                                doneHandler.handle(null);

                                return;
                            }

                            responseHandler.handle(event);
                        }
                    });
                    socket.dataHandler(responseParser);
                    socket.exceptionHandler(new Handler<Throwable>() {
                        @Override
                        public void handle(Throwable event) {
                            container.logger().error("Socket error from test client", event);
                        }
                    });

                    Pump.createPump(new DataStream(buffer, repeat, sendCounter), socket).start();



                    socket.closeHandler(new Handler<Void>() {
                        @Override
                        public void handle(Void event) {
//                            getContainer().logger().info("Bloody hell");
                        }
                    });
                }
            });
        }
    }

    protected Buffer createFakeTrackingPacket() {
        final Buffer fakeTrackingPacket = new Buffer();
        fakeTrackingPacket.appendByte((byte) 0x01);
        fakeTrackingPacket.appendBytes(new byte[33]);
        return fakeTrackingPacket;
    }

    private static class DataStream implements ReadStream<Object> {
        private final Buffer buffer;
        private final int repeat;
        private final AtomicInteger sendCounter;
        private boolean paused;
        private int count = 0;
        private Handler<Buffer> dataHandler;

        public DataStream(Buffer buffer, int repeat, AtomicInteger sendCounter) {

            this.buffer = buffer;
            this.repeat = repeat;
            this.sendCounter = sendCounter;
        }

        @Override
        public Object endHandler(Handler<Void> endHandler) {
            return null;
        }

        @Override
        public Object dataHandler(Handler<Buffer> handler) {
            this.dataHandler = handler;
            if (dataHandler != null) doGenerateData();
            return this;
        }

        protected void doGenerateData() {
            while (!paused && count < repeat) {
                count++;
//                final int sendCount = sendCounter.addAndGet(buffer.length());
//                 if (sendCount % 1000 == 0) System.out.println("Generate data: " + sendCount);
                dataHandler.handle(buffer);

//                if (count == repeat) {
//                    System.out.println("Bytes sent: " + sendCounter.get());
//                }
            }
        }

        @Override
        public Object pause() {
            paused = true;
            return this;
        }

        @Override
        public Object resume() {
            paused = false;
            if (dataHandler != null) doGenerateData();
            return this;
        }

        @Override
        public Object exceptionHandler(Handler<Throwable> handler) {
            return null;
        }
    }
}
