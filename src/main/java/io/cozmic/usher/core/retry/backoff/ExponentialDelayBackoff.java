package io.cozmic.usher.core.retry.backoff;

import io.cozmic.usher.core.retry.RetryContext;

public class ExponentialDelayBackoff implements Backoff {

    private final long initialDelayMillis;
    private final double multiplier;

    public ExponentialDelayBackoff(long initialDelayMillis, double multiplier) {
        if (initialDelayMillis <= 0) {
            throw new IllegalArgumentException("Initial delay must be positive but was: " + initialDelayMillis);
        }
        this.initialDelayMillis = initialDelayMillis;
        this.multiplier = multiplier;
    }

    @Override
    public long delayMillis(RetryContext context) {
        return (long) (initialDelayMillis * Math.pow(multiplier, context.getRetryCount() - 1));
    }
}
