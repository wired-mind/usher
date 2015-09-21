package io.cozmic.usher.test;

import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;

/**
 * Created by chuck on 9/21/15.
 */
public class ErrFilter extends AbstractFilter {
    @Override
    protected void start(AsyncResultHandler<Void> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void handleRequest(PipelinePack pipelinePack, AsyncResultHandler<PipelinePack> asyncResultHandler) {
          asyncResultHandler.handle(Future.failedFuture(new RuntimeException("Oops, I failed.")));
    }
}
