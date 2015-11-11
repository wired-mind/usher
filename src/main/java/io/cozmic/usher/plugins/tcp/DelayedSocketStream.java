package io.cozmic.usher.plugins.tcp;

import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.AsyncWriteStream;
import io.cozmic.usher.streams.ClosableWriteStream;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 11/11/15.
 */
public class DelayedSocketStream  implements AsyncWriteStream<Buffer>, ClosableWriteStream<Buffer> {
    private final SocketPool socketPool;
    private Handler<Throwable> exceptionHandler;

    public DelayedSocketStream(SocketPool socketPool) {

        this.socketPool = socketPool;
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public AsyncWriteStream<Buffer> write(Buffer data, Future<Void> future, PipelinePack pipelinePack) {
        doWrite(data);
        if (future != null) future.complete();
        return this;
    }

    @Override
    public DelayedSocketStream exceptionHandler(Handler<Throwable> handler) {
        exceptionHandler = handler;
        return this;
    }

    @Override
    public WriteStream<Buffer> write(Buffer data) {
        doWrite(data);
        return this;
    }

    private void doWrite(Buffer data) {
        socketPool.borrowObject(asyncResult -> {
            if (asyncResult.failed()) {
                if (exceptionHandler != null) exceptionHandler.handle(asyncResult.cause());
                return;
            }
            final ClosableWriteStream<Buffer> sock = asyncResult.result();
            sock.write(data);
            socketPool.returnObject(sock);
        });
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
        //nothing to do. not really a streaming capable stream :)
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
        //nothing to do. not really a streaming capable stream :)
        return this;
    }


}
