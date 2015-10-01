package io.cozmic.usher.test;

import io.cozmic.usher.core.MessageInjector;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Created by chuck on 8/19/15.
 */
public class FakeFilter extends AbstractFilter {
    private PipelinePack lastPipelinePack;

    @Override
    public void handleRequest(PipelinePack pipelinePack, Future<Void> writeCompleteFuture, Handler<PipelinePack> dataHandler, MessageInjector messageInjector) {

        this.lastPipelinePack = pipelinePack;
        dataHandler.handle(pipelinePack);
        writeCompleteFuture.complete();
    }


    public PipelinePack getLastPipelinePack() {
        return lastPipelinePack;
    }
}
