package io.cozmic.usher.plugins.tcp;

import io.cozmic.usher.core.InputPlugin;
import io.cozmic.usher.pipeline.DuplexStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;

/**
 * Created by chuck on 6/26/15.
 */
public class TcpInput implements InputPlugin {

    private NetServer netServer;



    @Override
    public void run(AsyncResultHandler<Void> startupHandler, Handler<DuplexStream<Buffer, Buffer>> duplexStreamHandler) {

        netServer.connectHandler(socket -> {
            duplexStreamHandler.handle(new DuplexStream<>(socket, socket));
        });

        netServer.listen(netServerAsyncResult -> {
            if (netServerAsyncResult.failed()) {
                startupHandler.handle(Future.failedFuture(netServerAsyncResult.cause()));
                return;
            }

            startupHandler.handle(Future.succeededFuture());
        });
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {
        netServer = vertx.createNetServer(buildOptions(configObj));
    }

    private NetServerOptions buildOptions(JsonObject inputObj) {
        return new NetServerOptions(inputObj);
    }
}
