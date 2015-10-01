package io.cozmic.usher.test;

import io.cozmic.usher.core.MessageInjector;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Created by chuck on 9/21/15.
 */
public class ErrFilter extends AbstractFilter {

    @Override
    public void handleRequest(PipelinePack pipelinePack, Future<Void> writeCompleteFuture, Handler<PipelinePack> dataHandler, MessageInjector messageInjector) {
        writeCompleteFuture.fail(new RuntimeException("Oops, I failed."));
    }
}
