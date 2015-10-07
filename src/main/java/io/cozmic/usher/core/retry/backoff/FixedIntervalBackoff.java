package io.cozmic.usher.core.retry.backoff;


import io.cozmic.usher.core.retry.RetryContext;

/**
 * @author Tomasz Nurkiewicz
 * @since 7/16/13, 6:14 PM
 */
public class FixedIntervalBackoff implements Backoff {

    public static final long DEFAULT_PERIOD_MILLIS = 1000;

    private final long intervalMillis;

    public FixedIntervalBackoff() {
        this(DEFAULT_PERIOD_MILLIS);
    }

    public FixedIntervalBackoff(long intervalMillis) {
        this.intervalMillis = intervalMillis;
    }

    @Override
    public long delayMillis(RetryContext context) {
        return intervalMillis;
    }

}
