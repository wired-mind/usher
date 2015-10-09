package io.cozmic.usher.core;

import io.cozmic.usher.core.retry.AsyncRetryContext;
import io.cozmic.usher.core.retry.RetryPolicy;
import io.cozmic.usher.core.retry.backoff.Backoff;
import io.cozmic.usher.core.retry.backoff.ExponentialDelayBackoff;
import io.cozmic.usher.core.retry.backoff.FixedIntervalBackoff;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.Objects;

/**
 * Created by chuck on 10/7/15.
 */
public class RetryHandler {
    private final Vertx vertx;
    private final boolean fixedDelay;
    private final RetryPolicy retryPolicy;
    private final Backoff backoff;


    public RetryHandler(Vertx vertx) {
        this(vertx, RetryPolicy.DEFAULT, Backoff.DEFAULT);
    }

    public RetryHandler(Vertx vertx, Backoff backoff) {
        this(vertx, RetryPolicy.DEFAULT, backoff);
    }

    public RetryHandler(Vertx vertx, RetryPolicy retryPolicy) {
        this(vertx, retryPolicy, Backoff.DEFAULT);
    }

    public RetryHandler(Vertx vertx, RetryPolicy retryPolicy, Backoff backoff) {
        this(vertx, retryPolicy, backoff, false);
    }


    public RetryHandler(Vertx vertx, RetryPolicy retryPolicy, Backoff backoff, boolean fixedDelay) {
        this.vertx = Objects.requireNonNull(vertx);
        this.retryPolicy = Objects.requireNonNull(retryPolicy);
        this.backoff = Objects.requireNonNull(backoff);
        this.fixedDelay = fixedDelay;
    }

    public boolean isFixedDelay() {
        return fixedDelay;
    }

    public RetryHandler retryOn(Class<Throwable> retryOnThrowables) {
        return this.withRetryPolicy(retryPolicy.retryOn(retryOnThrowables));
    }

    public RetryHandler withRetryPolicy(RetryPolicy retryPolicy) {
        return new RetryHandler(vertx, retryPolicy, backoff, fixedDelay);
    }
    public RetryHandler withBackoff(Backoff backoff) {
        return new RetryHandler(vertx, retryPolicy, backoff, fixedDelay);
    }
    public RetryHandler withUniformJitter() {
        return this.withBackoff(this.backoff.withUniformJitter());
    }

    public RetryHandler withMaxRetries(int times) {
        return this.withRetryPolicy(this.retryPolicy.withMaxRetries(times));
    }

    public RetryHandler retryInfinitely() {
        return this.withMaxRetries(Integer.MAX_VALUE);
    }

    public RetryHandler withExponentialBackoff(long initialDelayMillis, double multiplier) {
        final ExponentialDelayBackoff backoff = new ExponentialDelayBackoff(initialDelayMillis, multiplier);
        return new RetryHandler(vertx, retryPolicy, backoff, fixedDelay);
    }

    public RetryHandler withFixedBackoff(long delayMillis) {
        final FixedIntervalBackoff backoff = new FixedIntervalBackoff(delayMillis);
        return new RetryHandler(vertx, retryPolicy, backoff, fixedDelay);
    }

    public RetryHandler withMaxDelay(long maxDelayMillis) {
        return this.withBackoff(this.backoff.withMaxDelay(maxDelayMillis));
    }

    public void runWithRetry(Handler<AsyncRetryContext> handler, Handler<AsyncResult<Void>> doneHandler) {
        final AsyncRetryContext asyncRetryContext = new AsyncRetryContext(retryPolicy);

        doWithRetry(System.currentTimeMillis(), handler, asyncRetryContext, doneHandler);
    }

    private void doWithRetry(long startTime, Handler<AsyncRetryContext> handler, AsyncRetryContext asyncRetryContext, Handler<AsyncResult<Void>> doneHandler) {
        asyncRetryContext.setHandler(asyncResult -> {
            if (asyncResult.failed()) {
                final AsyncRetryContext nextRetry = asyncRetryContext.nextRetry(asyncResult.cause());
                final boolean tryAgain = retryPolicy.shouldContinue(nextRetry);
                if (!tryAgain) {
                    doneHandler.handle(Future.failedFuture("Giving up retry."));
                    return;
                }

                vertx.setTimer(calculateNextDelay(System.currentTimeMillis() - startTime, nextRetry, backoff), timerId -> {
                    doWithRetry(System.currentTimeMillis(), handler, nextRetry, doneHandler);
                });
                return;
            }

            doneHandler.handle(Future.succeededFuture());
        });

        handler.handle(asyncRetryContext);
    }

    private long calculateNextDelay(long taskDurationMillis, AsyncRetryContext nextRetryContext, Backoff backoff) {
        final long delay = backoff.delayMillis(nextRetryContext);
        return delay - (isFixedDelay()? taskDurationMillis : 0);
    }
}
