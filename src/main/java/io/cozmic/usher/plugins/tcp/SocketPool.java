package io.cozmic.usher.plugins.tcp;

import io.cozmic.usher.core.WriteStreamPool;
import io.cozmic.usher.streams.ClosableWriteStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;

/**
 * Created by chuck on 6/30/15.
 */
public class SocketPool extends WriteStreamPool {
    Logger logger = LoggerFactory.getLogger(SocketPool.class.getName());
    private String host;
    private NetClient netClient;
    private Integer port;

    public SocketPool(JsonObject configObj, Vertx vertx) {
        super(configObj, vertx);
    }


    @Override
    protected void initialize(int minIdle) {
        host = configObj.getString("host");
        port = Integer.parseInt(configObj.getValue("port").toString());
        netClient = vertx.createNetClient(buildOptions(configObj));
        super.initialize(minIdle);
    }

    private NetClientOptions buildOptions(JsonObject configObj) {
        return new NetClientOptions(configObj);
    }

    @Override
    protected void destroyObject(ClosableWriteStream<Buffer> obj) {
        obj.close();
    }

    @Override
    protected void createObject(AsyncResultHandler<ClosableWriteStream<Buffer>> readyHandler) {
        netClient.connect(port, host, connectHandler -> {
            if (connectHandler.failed()) {
                final Throwable cause = connectHandler.cause();
                logger.error(cause.getMessage(), cause);
                readyHandler.handle(Future.failedFuture(cause));
                return;
            }
            readyHandler.handle(Future.succeededFuture(new SocketWriteStream(connectHandler.result())));
        });
    }

    protected Class className() {
        return SocketPool.class;
    }
}
