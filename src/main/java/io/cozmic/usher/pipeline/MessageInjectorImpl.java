package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.MessageInjector;
import io.cozmic.usher.core.MessageMatcher;
import io.cozmic.usher.core.StreamMux;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Created by chuck on 9/30/15.
 *
 * Filters can use the MessageInjector to send inject new messages into the pipeline.
 *
 * We could have done this using a buffering queue, but rather instead we provide an AsyncResult handler
 * allowing the filter that injects to delay responding until the injected message is sent.
 *
 * At a later date a buffering queue overload might be nice, but for now our primary use case is Kafka which
 * is acting as our queue.
 */
public class MessageInjectorImpl implements MessageInjector {
    private final StreamMux mux;
    private final MessageMatcher messageMatcher;

    public MessageInjectorImpl(StreamMux mux, MessageMatcher messageMatcher) {
        this.mux = mux;
        this.messageMatcher = messageMatcher;
    }

    @Override
    public void inject(PipelinePack pipelinePack, Handler<AsyncResult<PipelinePack>> doneHandler) {
        if (messageMatcher.matches(pipelinePack)) doneHandler.handle(Future.failedFuture("Cannot inject a message that will match on the current filter. Check your filters and make sure to exclude this message from the messageMatcher of the this filter."));
        mux.write(pipelinePack, doneHandler);
    }
}
