package io.cozmic.usher.plugins.tcp;

import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.OutputPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.streams.ClosableWriteStream;
import io.cozmic.usher.streams.DuplexStream;
import io.cozmic.usher.streams.NullReadStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;


/**
 * Created by chuck on 6/29/15.
 */
public class TcpOutput implements OutputPlugin {

    Logger logger = LoggerFactory.getLogger(TcpOutput.class.getName());
    private SocketPool socketPool;
    private JsonObject configObj;
    private Boolean keepAlive;

    @Override
    public void init(JsonObject configObj, Vertx vertx) {
        this.configObj = configObj;
        socketPool = new SocketPool(configObj, vertx);
        keepAlive = configObj.getBoolean("keepAlive", true);
    }

    @Override
    public void run(AsyncResultHandler<DuplexStream<Buffer, Buffer>> duplexStreamAsyncResultHandler) {
        if (keepAlive) {
            createKeepAliveStream(duplexStreamAsyncResultHandler);
            return;
        }

        createStream(duplexStreamAsyncResultHandler);
    }

    private void createStream(AsyncResultHandler<DuplexStream<Buffer, Buffer>> duplexStreamAsyncResultHandler) {
        final DelayedSocketStream stream = new DelayedSocketStream(socketPool);
        final DuplexStream<Buffer, Buffer> duplexStream = new DuplexStream<>(NullReadStream.getInstance(), stream);
        duplexStreamAsyncResultHandler.handle(Future.succeededFuture(duplexStream));
    }

    private void createKeepAliveStream(AsyncResultHandler<DuplexStream<Buffer, Buffer>> duplexStreamAsyncResultHandler) {
        socketPool.borrowObject(asyncResult -> {
            if (asyncResult.failed()) {
                final Throwable cause = asyncResult.cause();
                logger.error(String.format("Unable to obtain socket for %s. Cause: %s", configObj.toString(), cause.getMessage()), cause);
                run(duplexStreamAsyncResultHandler);
                return;
            }

            final ClosableWriteStream<Buffer> writeStream = asyncResult.result();
            SocketWriteStream socketWriteStream = (SocketWriteStream) writeStream;
            NetSocket socket = socketWriteStream.getSocket();

            final DuplexStream<Buffer, Buffer> duplexStream =
                    new DuplexStream<>(socket, writeStream)
                            .closeHandler(v -> {
                                socket.close();
                            })
                            .packDecorator(pack -> {
                                final Message message = pack.getMessage();
                                message.setRemoteAddress(socket.remoteAddress());
                                message.setLocalAddress(socket.localAddress());
                            });

            duplexStreamAsyncResultHandler.handle(Future.succeededFuture(duplexStream));
        });
    }

    @Override
    public void stop(OutPipeline outPipeline) {
        outPipeline.stop(socketPool);
    }


}
