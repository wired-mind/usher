package io.cozmic.usher.pipeline;

import io.vertx.core.AsyncResultHandler;

/**
 * Created by chuck on 6/25/15.
 */
public interface OutputRunner {
    void run(AsyncResultHandler<MessageFilteringStream> messageFilteringStreamAsyncResultHandler);

    void stop(MessageFilter messageFilter);
}
