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
    private final Buffer response;
    private final int port;

    Logger logger = LoggerFactory.getLogger(FakeService.class.getName());

    public FakeService(Buffer response) {
        this(response, FAKE_SERVICE_PORT);
    }

    public FakeService(Buffer response, int port) {

        this.response = response;
        this.port = port;
    }

    public void start(final Future<Void> startedResult)
    {

//        final Buffer fakeTrackingResponse = Buffer.buffer();
//        fakeTrackingResponse.appendByte((byte) 0x11);
//        fakeTrackingResponse.appendByte((byte) 0x01);

        final NetServer netServer = vertx.createNetServer();

        netServer
                .connectHandler(socket -> {
                    socket.exceptionHandler(event -> {
                        logger.error("Socket error on fake service socket", event);
                    });
                    socket.handler(event -> {
                        socket.write(response);
                    });


                })
                .listen(port, FAKE_SERVICE_HOST, event -> {
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
