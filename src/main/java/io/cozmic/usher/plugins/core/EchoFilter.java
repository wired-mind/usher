package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.MessageInjector;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Simplest example filter that does an Echo. Not implementing flow control.
 */
public class EchoFilter extends AbstractFilter {

    @Override
    public void handleRequest(PipelinePack pipelinePack, Future<Void> writeCompleteFuture, Handler<PipelinePack> dataHandler, MessageInjector messageInjector) {
        //would normally mutate the pipelinePack or create a new response pipelinePack
        dataHandler.handle(pipelinePack);
        writeCompleteFuture.complete();
    }
}
