package io.cozmic.usher.core;

import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;


/**
 * Created by chuck on 6/25/15.
 */
public interface InputPlugin extends Plugin {
    /**
     * The way the code works now it's expected that the duplexStreamHandler not be invoked immediately. Usually this
     * isn't a problem since IO is async and creating the server stream requires async callbacks. However, if you
     * are writing a plugin that can create the streams during the same "tick" as the run call, please invoke
     * the handler using vertx.runOnContext() to get the next "tick".
     * @param startupHandler
     * @param duplexStreamHandler
     */
    void run(AsyncResultHandler<Void> startupHandler, Handler<DuplexStream<Buffer, Buffer>> duplexStreamHandler);

    void stop(AsyncResultHandler<Void> stopHandler);
}
