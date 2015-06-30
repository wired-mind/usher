package io.cozmic.usher;

import io.cozmic.usherprotocols.core.CozmicSocket;
import io.cozmic.usherprotocols.core.CozmicStreamProcessor;
import io.cozmic.usherprotocols.core.Message;
import io.cozmic.usherprotocols.core.Request;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.streams.Pump;


/**
 * Created by chuck on 10/27/14.
 */


public class EchoChamber extends AbstractVerticle {
    public static final int ECHO_SERVICE_PORT = 9193;
    public static final String ECHO_SERVICE_HOST = "localhost";
    Logger logger = LoggerFactory.getLogger(EchoChamber.class.getName());

    public void start(final Future<Void> startedResult) {


        final Integer delay = config().getInteger("delay", 1);

        final NetServer netServer = vertx.createNetServer();
        logger.info("Echo is Hello world!");
        netServer
                .connectHandler(new Handler<NetSocket>() {
                    @Override
                    public void handle(final NetSocket socket) {
                        socket.exceptionHandler(new Handler<Throwable>() {
                            @Override
                            public void handle(Throwable event) {
                                logger.error("Socket error on echo service socket", event);
                            }
                        });
                        final CozmicSocket cozmicSocket = new CozmicSocket(socket);
                        final Pump pump = Pump.pump(cozmicSocket.translate(new CozmicStreamProcessor() {
                            @Override
                            public void process(Message message, AsyncResultHandler<Message> resultHandler) {
                                try {
                                    final Request request = Request.fromEnvelope(message.buildEnvelope());
                                    final Buffer body = request.getBody();
                                    final Message reply = message.createReply(body);
                                    resultHandler.handle(Future.succeededFuture(reply));
                                } catch (Exception ex) {
                                    resultHandler.handle(Future.failedFuture(ex));
                                }
                            }
                        }), cozmicSocket);
                        pump.start();
                    }
                })
                .listen(ECHO_SERVICE_PORT, ECHO_SERVICE_HOST, new Handler<AsyncResult<NetServer>>() {
                    @Override
                    public void handle(AsyncResult<NetServer> event) {
                        if (event.failed()) {
                            logger.error(event.cause().getMessage());
                            startedResult.fail(event.cause());
                            return;
                        }
                        logger.info(String.format("Started echo server - %s", ECHO_SERVICE_PORT));
                        startedResult.complete();
                    }
                });


    }
}
