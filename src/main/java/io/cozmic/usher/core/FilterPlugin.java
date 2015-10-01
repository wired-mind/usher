package io.cozmic.usher.core;

import io.cozmic.usher.pipeline.MessageInjectorImpl;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;

/**
 * Created by chuck on 7/6/15.
 */
public interface FilterPlugin extends Plugin {
    void run(MessageInjector messageInjector, AsyncResultHandler<MessageStream> messageStreamAsyncResultHandler);
}
