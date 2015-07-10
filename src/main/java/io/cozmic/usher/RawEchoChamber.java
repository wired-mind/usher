package io.cozmic.usher;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.streams.Pump;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by chuck on 10/27/14.
 */


public class RawEchoChamber extends AbstractVerticle {
    public static final int ECHO_SERVICE_PORT = 9193;
    public static final String ECHO_SERVICE_HOST = "localhost";
    Logger logger = LoggerFactory.getLogger(RawEchoChamber.class.getName());

    public void start(final Future<Void> startedResult) {


        final Integer delay = config().getInteger("delay", 1);

        final NetServer netServer = vertx.createNetServer(new NetServerOptions().setAcceptBacklog(10000));
        logger.info("Echo is Hello world!");
        netServer
                .connectHandler(socket -> {
                    socket.exceptionHandler(event -> {
                        logger.error("Socket error on echo service socket", event);
                    });


                    Pump.pump(socket, socket).start();

                })
                .listen(ECHO_SERVICE_PORT, ECHO_SERVICE_HOST, event -> {
                    if (event.failed()) {
                        final Throwable cause = event.cause();
                        logger.error(cause.getMessage(), cause);
                        startedResult.fail(cause);
                        return;
                    }
                    logger.info(String.format("Started echo server - %s", ECHO_SERVICE_PORT));
                    startedResult.complete();
                });


    }
}
