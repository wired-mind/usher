package io.cozmic.usher.plugins.core;

import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;

/**
 * Simplest example filter that does an Echo. Not implementing flow control.
 */
public class EchoFilter extends AbstractFilter {
    @Override
    public void handleRequest(PipelinePack pipelinePack, AsyncResultHandler<PipelinePack> asyncResultHandler) {
        //would normally mutate the pipelinePack or create a new response pipelinePack
        asyncResultHandler.handle(Future.succeededFuture(pipelinePack));
    }
}
