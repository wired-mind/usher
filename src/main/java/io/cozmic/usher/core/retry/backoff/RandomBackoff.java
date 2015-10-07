package io.cozmic.usher.core.retry.backoff;


import io.cozmic.usher.core.retry.RetryContext;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;


abstract public class RandomBackoff extends BackoffWrapper {

    private final Supplier<Random> randomSource;

    protected RandomBackoff(Backoff target) {
        this(target, ThreadLocalRandom::current);
    }

    protected RandomBackoff(Backoff target, Random randomSource) {
        this(target, () -> randomSource);
    }

    private RandomBackoff(Backoff target, Supplier<Random> randomSource) {
        super(target);
        this.randomSource = randomSource;
    }

    @Override
    public long delayMillis(RetryContext context) {
        final long initialDelay = target.delayMillis(context);
        final long randomDelay = addRandomJitter(initialDelay);
        return Math.max(randomDelay, 0);
    }

    abstract long addRandomJitter(long initialDelay);

    protected Random random() {
        return randomSource.get();
    }
}
