package io.cozmic.usherprotocols.core;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;

/**
 * Created by chuck on 9/30/14.
 */
public interface StreamProcessor {
    void process(Buffer buffer, Handler<AsyncResult<Buffer>> resultHandler);
}
