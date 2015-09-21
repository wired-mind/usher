package io.cozmic.usher.test;

import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;

/**
 * Created by chuck on 8/19/15.
 */
public class FakeFilter extends AbstractFilter {
    private PipelinePack lastPipelinePack;


    @Override
    protected void start(AsyncResultHandler<Void> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void handleRequest(PipelinePack pipelinePack, AsyncResultHandler<PipelinePack> asyncResultHandler) {

        this.lastPipelinePack = pipelinePack;
        asyncResultHandler.handle(Future.succeededFuture(pipelinePack));

    }

    public PipelinePack getLastPipelinePack() {
        return lastPipelinePack;
    }
}
