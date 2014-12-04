package io.cozmic.usher;

import io.cozmic.usherprotocols.core.CozmicSocket;
import io.cozmic.usherprotocols.core.CozmicStreamProcessor;
import io.cozmic.usherprotocols.core.Message;
import io.cozmic.usherprotocols.core.Request;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.streams.Pump;
import org.vertx.java.platform.Verticle;

/**
 * Created by chuck on 10/27/14.
 */


public class EchoChamber extends Verticle {
    public static final int ECHO_SERVICE_PORT = 9193;
    public static final String ECHO_SERVICE_HOST = "localhost";

    public void start(final Future<Void> startedResult) {


        final Integer delay = container.config().getInteger("delay", 1);

        final NetServer netServer = vertx.createNetServer();
            container.logger().info("Echo is Hello world!");
        netServer
                .connectHandler(new Handler<NetSocket>() {
                    @Override
                    public void handle(final NetSocket socket) {
                        socket.exceptionHandler(new Handler<Throwable>() {
                            @Override
                            public void handle(Throwable event) {
                                container.logger().error("Socket error on echo service socket", event);
                            }
                        });
                        final CozmicSocket cozmicSocket = new CozmicSocket(socket);
                        final Pump pump = Pump.createPump(cozmicSocket.translate(new CozmicStreamProcessor() {
                            @Override
                            public void process(Message message, AsyncResultHandler<Message> resultHandler) {
                                try {
                                    final Request request = Request.fromEnvelope(message.buildEnvelope());
                                    final Buffer body = request.getBody();
                                    final Message reply = message.createReply(body);
                                    resultHandler.handle(new DefaultFutureResult<>(reply));
                                } catch (Exception ex) {
                                    resultHandler.handle(new DefaultFutureResult(ex));
                                }
                            }
                        }), cozmicSocket);
                        pump.start();
                    }
                })
                .setAcceptBacklog(10000)
                .listen(ECHO_SERVICE_PORT, ECHO_SERVICE_HOST, new Handler<AsyncResult<NetServer>>() {
                    @Override
                    public void handle(AsyncResult<NetServer> event) {
                        if (event.failed()) {
                            container.logger().error(event.cause().getMessage());
                            startedResult.setFailure(event.cause());
                            return;
                        }
                        container.logger().info(String.format("Started echo server - %s", ECHO_SERVICE_PORT));
                        startedResult.setResult(null);
                    }
                });

        vertx.eventBus().registerHandler("STOP_ECHO_CHAMBER", new Handler<org.vertx.java.core.eventbus.Message>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message event) {
                netServer.close();
            }
        });

        vertx.eventBus().registerHandler("START_ECHO_CHAMBER", new Handler<org.vertx.java.core.eventbus.Message>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message event) {
                netServer.listen(ECHO_SERVICE_PORT, ECHO_SERVICE_HOST, new Handler<AsyncResult<NetServer>>() {
                    @Override
                    public void handle(AsyncResult<NetServer> event) {
                        if (event.failed()) {
                            container.logger().error(event.cause().getMessage());
                        }


                    }
                });
            }
        });
    }
}
