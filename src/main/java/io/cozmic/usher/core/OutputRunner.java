package io.cozmic.usher.core;

import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;

/**
 * Created by chuck on 6/25/15.
 */
public interface OutputRunner {
    void run(AsyncResultHandler<MessageStream> messageStreamAsyncResultHandler);

    void stop(MessageStream messageStream);
}
