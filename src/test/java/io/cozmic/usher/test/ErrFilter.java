package io.cozmic.usher.test;

import io.cozmic.usher.core.MessageInjector;
import io.cozmic.usher.core.retry.RetryContext;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by chuck on 9/21/15.
 */
public class ErrFilter extends AbstractFilter {

    private Integer repeatErrorCount;
    private Integer delay;

    public void start() {
        repeatErrorCount = getConfigObj().getInteger("repeatErrorCount", 10);
        delay = getConfigObj().getInteger("delay", 1);
    }

    @Override
    public void handleRequest(PipelinePack pipelinePack, Future<Void> writeCompleteFuture, Handler<PipelinePack> dataHandler, MessageInjector messageInjector) {
        final RetryContext retryContext = pipelinePack.getRetryContext();
        final int retryCount = retryContext.getRetryCount();

        final boolean isErrorMode = retryCount <= repeatErrorCount;
        if (isErrorMode) {
            getVertx().setTimer(delay, timerId -> writeCompleteFuture.fail("Oops, I failed."));
            return;
        }

        writeCompleteFuture.complete();
    }
}
