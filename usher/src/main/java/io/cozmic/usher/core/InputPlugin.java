package io.cozmic.usher.core;

import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;


/**
 * Created by chuck on 6/25/15.
 */
public interface InputPlugin extends Plugin {
    void run(AsyncResultHandler<Void> startupHandler, Handler<DuplexStream<Buffer, Buffer>> duplexStreamHandler);
}
