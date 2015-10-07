package io.cozmic.usher.test;

import io.cozmic.usher.core.MessageInjector;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by chuck on 9/21/15.
 */
public class ErrFilter extends AbstractFilter {

    private AtomicInteger errorCounter = new AtomicInteger();
    private Integer repeatErrorCount;
    private Integer delay;

    public void start() {
        repeatErrorCount = getConfigObj().getInteger("repeatErrorCount", 10);
        delay = getConfigObj().getInteger("delay", 1);
    }

    @Override
    public void handleRequest(PipelinePack pipelinePack, Future<Void> writeCompleteFuture, Handler<PipelinePack> dataHandler, MessageInjector messageInjector) {
        final int errorCount = errorCounter.incrementAndGet();
        final boolean isErrorMode = errorCount <= repeatErrorCount;
        if (isErrorMode) {
            getVertx().setTimer(delay, timerId -> writeCompleteFuture.fail(new RuntimeException("Oops, I failed.")));
            return;
        }

        writeCompleteFuture.complete();
    }
}
