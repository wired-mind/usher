package io.cozmic.usher.streams;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * Created by chuck on 10/1/15.
 */
public class NullReadStream implements ReadStream<Buffer> {

    private static ReadStream<Buffer> instance = new NullReadStream();
    public static ReadStream<Buffer> getInstance() {
        return instance;
    }

    @Override
    public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
        return this;
    }

    @Override
    public ReadStream<Buffer> handler(Handler<Buffer> handler) {
        return this;
    }

    @Override
    public ReadStream<Buffer> pause() {
        return this;
    }

    @Override
    public ReadStream<Buffer> resume() {
        return this;
    }

    @Override
    public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
        return this;
    }
}
