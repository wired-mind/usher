package io.cozmic.usher.plugins.tcp;

import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.OutputPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;
import io.vertx.core.streams.WriteStream;


/**
 * Created by chuck on 6/29/15.
 */
public class TcpOutput implements OutputPlugin {

    Logger logger = LoggerFactory.getLogger(TcpOutput.class.getName());
    private SocketPool socketPool;
    private JsonObject configObj;

    @Override
    public void init(JsonObject configObj, Vertx vertx) {
        this.configObj = configObj;
        socketPool = new SocketPool(configObj, vertx);
    }

    @Override
    public void run(AsyncResultHandler<DuplexStream<Buffer, Buffer>> duplexStreamAsyncResultHandler) {
        socketPool.borrowObject(asyncResult -> {
            if (asyncResult.failed()) {
                final Throwable cause = asyncResult.cause();
                logger.error(String.format("Unable to obtain socket for %s. Cause: %s", configObj.toString(), cause.getMessage()), cause);
                run(duplexStreamAsyncResultHandler);
                return;
            }

            final WriteStream<Buffer> writeStream = asyncResult.result();
            NetSocket socket = (NetSocket)writeStream;
            duplexStreamAsyncResultHandler.handle(Future.succeededFuture(new DuplexStream<>(socket, socket, pack -> {
                final Message message = pack.getMessage();
                message.setRemoteAddress(socket.remoteAddress());
                message.setLocalAddress(socket.localAddress());
            }, v->{socket.close();})));
        });

    }

    @Override
    public void stop(OutPipeline outPipeline) {
        outPipeline.stop(socketPool);
    }



}
