package io.cozmic.usher.core.retry.backoff;


import io.cozmic.usher.core.retry.RetryContext;

public class BoundedMaxBackoff extends BackoffWrapper {

    public static final long DEFAULT_MAX_DELAY_MILLIS = 10_000;

    private final long maxDelayMillis;

    public BoundedMaxBackoff(Backoff target) {
        this(target, DEFAULT_MAX_DELAY_MILLIS);
    }

    public BoundedMaxBackoff(Backoff target, long maxDelayMillis) {
        super(target);
        this.maxDelayMillis = maxDelayMillis;
    }

    @Override
    public long delayMillis(RetryContext context) {
        return Math.min(target.delayMillis(context), maxDelayMillis);
    }
}
