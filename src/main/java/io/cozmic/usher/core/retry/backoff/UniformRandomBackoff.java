package io.cozmic.usher.core.retry.backoff;

import java.util.Random;


public class UniformRandomBackoff extends RandomBackoff {

    /**
     * Randomly between +/- 100ms
     */
    public static final long DEFAULT_RANDOM_RANGE_MILLIS = 100;

    private final long range;

    public UniformRandomBackoff(Backoff target) {
        this(target, DEFAULT_RANDOM_RANGE_MILLIS);
    }

    public UniformRandomBackoff(Backoff target, Random random) {
        this(target, DEFAULT_RANDOM_RANGE_MILLIS, random);
    }

    public UniformRandomBackoff(Backoff target, final long range) {
        super(target);
        this.range = range;
    }

    public UniformRandomBackoff(Backoff target, final long range, Random random) {
        super(target, random);
        this.range = range;
    }

    @Override
    long addRandomJitter(long initialDelay) {
        final double uniformRandom = (1 - random().nextDouble() * 2) * range;
        return (long) (initialDelay + uniformRandom);
    }


}
