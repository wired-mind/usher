package io.cozmic.usherprotocols.core;


import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 9/30/14.
 */
public interface StreamProcessor {
    void process(Buffer buffer, Handler<AsyncResult<Buffer>> resultHandler);
}
