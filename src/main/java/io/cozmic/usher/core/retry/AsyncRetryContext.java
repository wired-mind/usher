package io.cozmic.usher.core.retry;


import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.Objects;


public class AsyncRetryContext implements RetryContext, Handler<AsyncResult<Void>> {

    private final RetryPolicy retryPolicy;
    private final int retry;
    private final Throwable lastThrowable;
    private final Future<PipelinePack> future;

    public AsyncRetryContext(RetryPolicy retryPolicy) {
        this(retryPolicy, 0, null);
    }

    public AsyncRetryContext(RetryPolicy retryPolicy, int retry, Throwable lastThrowable) {
        this.retryPolicy = Objects.requireNonNull(retryPolicy);
        this.retry = retry;
        this.lastThrowable = lastThrowable;
        this.future = Future.future();
    }

    @Override
    public boolean willRetry() {
        return retryPolicy.shouldContinue(this.nextRetry(new Exception()));
    }

    @Override
    public int getRetryCount() {
        return retry;
    }

    @Override
    public Throwable getLastThrowable() {
        return lastThrowable;
    }

    public AsyncRetryContext nextRetry(Throwable cause) {
        return new AsyncRetryContext(retryPolicy, retry + 1, cause);
    }

    public AsyncRetryContext prevRetry() {
        return new AsyncRetryContext(retryPolicy, retry - 1, lastThrowable);
    }

    public void setHandler(Handler<AsyncResult<PipelinePack>> handler) {
        future.setHandler(handler);
    }

    @Override
    public void handle(AsyncResult<Void> asyncResult) {
        if (asyncResult.failed()) {
            future.fail(asyncResult.cause());
            return;
        }

        future.complete();
    }
}
