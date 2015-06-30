package io.cozmic.usher.pipeline;

import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;

/**
 * Created by chuck on 6/25/15.
 */
public interface InputRunner {
    public void start(AsyncResultHandler<Void> startupHandler, Handler<MessageParsingStream> messageParsingStreamHandler);
}
