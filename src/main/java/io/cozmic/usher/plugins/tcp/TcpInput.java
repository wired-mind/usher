package io.cozmic.usher.plugins.tcp;

import io.cozmic.usher.core.InputPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;

/**
 * Created by chuck on 6/26/15.
 */
public class TcpInput implements InputPlugin {
    Logger logger = LoggerFactory.getLogger(TcpInput.class.getName());

    private NetServer netServer;
    private JsonObject configObj;
    private Vertx vertx;


    @Override
    public void run(AsyncResultHandler<Void> startupHandler, Handler<DuplexStream<Buffer, Buffer>> duplexStreamHandler) {

        netServer.connectHandler(socket -> {

            duplexStreamHandler.handle(new DuplexStream<>(socket, socket, pack -> {
                final Message message = pack.getMessage();
                message.setRemoteAddress(socket.remoteAddress());
                message.setLocalAddress(socket.localAddress());
            }, v->{socket.close();}));
        });

        netServer.listen(netServerAsyncResult -> {
            if (netServerAsyncResult.failed()) {
                startupHandler.handle(Future.failedFuture(netServerAsyncResult.cause()));
                return;
            }
            logger.info("Tcp Server started: " + configObj);
            startupHandler.handle(Future.succeededFuture());
        });
    }


    @Override
    public void init(JsonObject configObj, Vertx vertx) {
        this.configObj = configObj;
        this.vertx = vertx;
        netServer = vertx.createNetServer(buildOptions(configObj));
    }

    private NetServerOptions buildOptions(JsonObject configObj) {
        return new NetServerOptions(configObj);
    }
}
