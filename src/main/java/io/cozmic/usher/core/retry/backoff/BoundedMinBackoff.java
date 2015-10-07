package io.cozmic.usher.core.retry.backoff;


import io.cozmic.usher.core.retry.RetryContext;

public class BoundedMinBackoff extends BackoffWrapper {

    public static final long DEFAULT_MIN_DELAY_MILLIS = 100;

    private final long minDelayMillis;

    public BoundedMinBackoff(Backoff target) {
        this(target, DEFAULT_MIN_DELAY_MILLIS);
    }

    public BoundedMinBackoff(Backoff target, long minDelayMillis) {
        super(target);
        this.minDelayMillis = minDelayMillis;
    }

    @Override
    public long delayMillis(RetryContext context) {
        return Math.max(target.delayMillis(context), minDelayMillis);
    }
}
