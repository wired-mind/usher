package io.cozmic.usher.test.integration;

import io.cozmic.usher.core.ProxyTunnel;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;
import org.vertx.java.core.streams.Pump;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.platform.Verticle;

/**
 * Created by chuck on 10/3/14.
 */
public class CozmicBot extends Verticle {
    public static final String COZMICBOT_RESULT = "COZMICBOT_RESULT";

    private NetClient netClient;

    public void start(final Future<Void> startedResult) {

        final int repeat = container.config().getNumber("repeat", 1).intValue();
        final byte[] bytes = container.config().getBinary("buffer", new byte[]{});
        final Buffer buffer = new Buffer(bytes);
        netClient = vertx.createNetClient().setConnectTimeout(5000).setReconnectAttempts(100).setReconnectInterval(1000);
        netClient.connect(ProxyTunnel.DEFAULT_PORT, ProxyTunnel.DEFAULT_HOST, new Handler<AsyncResult<NetSocket>>() {
            @Override
            public void handle(AsyncResult<NetSocket> event) {
                if (event.failed()) {
                    container.logger().error(event.cause().getMessage());
                    startedResult.setFailure(event.cause());
                    return;
                }



                final NetSocket socket = event.result();
                final RecordParser responseParser = RecordParser.newFixed(2, new Handler<Buffer>() {
                    int socketReceiveCount = 0;
                    @Override
                    public void handle(Buffer event) {


                        socketReceiveCount++;
                        if (socketReceiveCount == repeat) {
                            socket.dataHandler(null);
                            socket.close();
                        }

                        vertx.eventBus().send(COZMICBOT_RESULT, event);

                    }
                });
                socket.dataHandler(responseParser);
                socket.exceptionHandler(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable event) {
                        container.logger().error("Socket error from test client", event);
                    }
                });

                Pump.createPump(new DataStream(buffer, repeat), socket).start();



                socket.closeHandler(new Handler<Void>() {
                    @Override
                    public void handle(Void event) {
//                            getContainer().logger().info("Bloody hell");
                    }
                });
            }
        });

        startedResult.setResult(null);
    }



    private static class DataStream implements ReadStream<Object> {
        private final Buffer buffer;
        private final int repeat;
        private boolean paused;
        private int count = 0;
        private Handler<Buffer> dataHandler;

        public DataStream(Buffer buffer, int repeat) {

            this.buffer = buffer;
            this.repeat = repeat;
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
                dataHandler.handle(buffer);
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
