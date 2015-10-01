package io.cozmic.usher.core;

import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;

/**
 * Created by chuck on 6/25/15.
 */
public interface OutputRunner {
    void run(Handler<AsyncResult<MessageStream>> messageStreamAsyncResultHandler);

    void stop(MessageStream messageStream);
}
