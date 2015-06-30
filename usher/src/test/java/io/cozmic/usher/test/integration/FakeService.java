package io.cozmic.usher.test.integration;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;


/**
 * Created by chuck on 9/25/14.
 */
public class FakeService extends AbstractVerticle {

    public static final int FAKE_SERVICE_PORT = 9191;
    public static final String FAKE_SERVICE_HOST = "localhost";

    Logger logger = LoggerFactory.getLogger(FakeService.class.getName());

    public void start(final Future<Void> startedResult)
    {

        final Integer delay =  config().getInteger("delay", 1);
        final Buffer fakeTrackingResponse = Buffer.buffer();
        fakeTrackingResponse.appendByte((byte) 0x11);
        fakeTrackingResponse.appendByte((byte) 0x01);

        final NetServer netServer = vertx.createNetServer();

        netServer
                .connectHandler(socket -> {
                    socket.exceptionHandler(event -> {
                        logger.error("Socket error on fake service socket", event);
                    });
                    socket.handler(event -> {
                        socket.write(fakeTrackingResponse);
                    });


                })
                .listen(FAKE_SERVICE_PORT, FAKE_SERVICE_HOST, event -> {
                    if (event.failed()) {
                        final Throwable cause = event.cause();
                        logger.error(cause.getMessage());
                        startedResult.fail(cause);
                        return;
                    }

                    startedResult.complete();
                });
    }

}
