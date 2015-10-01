package io.cozmic.usher.streams;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 10/1/15.
 */
public class NullClosableWriteStream implements ClosableWriteStream<Buffer> {
    private static ClosableWriteStream<Buffer> instance = new NullClosableWriteStream();

    public static ClosableWriteStream<Buffer> getInstance() {
        return instance;
    }

    @Override
    public ClosableWriteStream<Buffer> write(Buffer data, Handler<AsyncResult<Void>> writeCompleteHandler) {
        return this;
    }

    @Override
    public ClosableWriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
        return this;
    }

    @Override
    public ClosableWriteStream<Buffer> write(Buffer data) {
        return this;
    }

    @Override
    public ClosableWriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public ClosableWriteStream<Buffer> drainHandler(Handler<Void> handler) {
        return this;
    }

    @Override
    public void close() {
        //no op
    }
}
