package io.cozmic.usher.core;

import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 7/6/15.
 */
public interface FilterPlugin extends Plugin {
    void run(AsyncResultHandler<DuplexStream<Buffer, Buffer>> duplexStreamAsyncResultHandler);
}
