package io.cozmic.usher.streams;

import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 10/1/15.
 */
public interface ClosableWriteStream<T> extends AsyncWriteStream<T> {
    void close();
}
