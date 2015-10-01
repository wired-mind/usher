package io.cozmic.usher.streams;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 10/1/15.
 */
public interface AsyncWriteStream<T> extends WriteStream<T> {
    public AsyncWriteStream<T> write(T data, Handler<AsyncResult<Void>> writeCompleteHandler);
}
