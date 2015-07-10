package io.cozmic.usher.test.integration;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;


/**
 * Created by chuck on 10/3/14.
 */
public class CozmicBot extends AbstractVerticle {
    public static final String COZMICBOT_RESULT = "COZMICBOT_RESULT";
    Logger logger = LoggerFactory.getLogger(CozmicBot.class.getName());

    private NetClient netClient;

    public void start(final Future<Void> startedResult) {

        final int repeat = config().getInteger("repeat", 1).intValue();
        final byte[] bytes = config().getBinary("buffer", new byte[]{});
        final Buffer buffer = Buffer.buffer(bytes);
        netClient = vertx.createNetClient();
//        .setConnectTimeout(5000).setReconnectAttempts(100).setReconnectInterval(1000);
        netClient.connect(2500, "localhost", new Handler<AsyncResult<NetSocket>>() {
            @Override
            public void handle(AsyncResult<NetSocket> event) {
                if (event.failed()) {
                    logger.error(event.cause().getMessage());
                    startedResult.fail(event.cause());
                    return;
                }



                final NetSocket socket = event.result();
                final RecordParser responseParser = RecordParser.newFixed(2, new Handler<Buffer>() {
                    int socketReceiveCount = 0;
                    @Override
                    public void handle(Buffer event) {


                        socketReceiveCount++;
                        if (socketReceiveCount == repeat) {
                            socket.handler(null);
                            socket.close();
                        }

                        vertx.eventBus().send(COZMICBOT_RESULT, event);

                    }
                });
                socket.handler(responseParser);
                socket.exceptionHandler(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable event) {
                        logger.error("Socket error from test client", event);
                    }
                });

                Pump.pump(new DataStream(buffer, repeat), socket).start();



                socket.closeHandler(new Handler<Void>() {
                    @Override
                    public void handle(Void event) {
//                            getContainer().logger().info("Bloody hell");
                    }
                });
            }
        });

        startedResult.complete();
    }



    private static class DataStream implements ReadStream<Buffer> {
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
        public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
            return this;
        }

        @Override
        public ReadStream<Buffer> handler(Handler<Buffer> handler) {
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
        public ReadStream<Buffer> pause() {
            paused = true;
            return this;
        }

        @Override
        public ReadStream<Buffer> resume() {
            paused = false;
            if (dataHandler != null) doGenerateData();
            return this;
        }

        @Override
        public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
            return this;
        }
    }
}
