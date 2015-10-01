package io.cozmic.usher.streams;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 10/1/15.
 *
 * The class is called "blocking" only because it assumes that the write is completed immediately after calling "write".
 * For our Tcp plugin, we're saying that is "good enough". The KafkaPlugin though will take advantage of the Async
 * interface differently.
 */
public class SocketWriteStream implements AsyncWriteStream<Buffer>, ClosableWriteStream<Buffer> {
    private final NetSocket innerStream;

    public SocketWriteStream(NetSocket stream) {
        innerStream = stream;
    }

    @Override
    public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
        innerStream.exceptionHandler(handler);
        return this;
    }

    @Override
    public WriteStream<Buffer> write(Buffer data) {
        innerStream.write(data);
        return this;
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
        innerStream.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return innerStream.writeQueueFull();
    }

    @Override
    public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
        innerStream.drainHandler(handler);
        return this;
    }

    @Override
    public AsyncWriteStream<Buffer> write(Buffer data, Handler<AsyncResult<Void>> writeCompleteHandler) {
        innerStream.write(data);
        if (writeCompleteHandler != null) writeCompleteHandler.handle(Future.succeededFuture());
        return this;
    }

    public void close() {
        innerStream.close();
    }

    public NetSocket getSocket() {
        return innerStream;
    }
}
