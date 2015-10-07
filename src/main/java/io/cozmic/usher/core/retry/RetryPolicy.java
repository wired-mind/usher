package io.cozmic.usher.core.retry;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Created by chuck on 10/7/15.
 */
public class RetryPolicy {
    public static final RetryPolicy DEFAULT = new RetryPolicy();
    private final int maxRetries;
    private final Set<Class<? extends Throwable>> retryOn;
    private final Set<Class<? extends Throwable>> abortOn;
    private final Predicate<Throwable> retryPredicate;
    private final Predicate<Throwable> abortPredicate;

    public RetryPolicy(int maxRetries, Set<Class<? extends Throwable>> retryOn, Set<Class<? extends Throwable>> abortOn, Predicate<Throwable> retryPredicate, Predicate<Throwable> abortPredicate) {
        this.maxRetries = maxRetries;
        this.retryOn = retryOn;
        this.abortOn = abortOn;
        this.retryPredicate = retryPredicate;
        this.abortPredicate = abortPredicate;
    }

    public RetryPolicy() {
        this(Integer.MAX_VALUE, Collections.emptySet(), Collections.emptySet(), th -> false, th -> false);
    }

    public RetryPolicy retryOn(Class<? extends Throwable>... retryOnThrowables) {
        return new RetryPolicy(maxRetries, setPlusElems(retryOn, retryOnThrowables), abortOn, retryPredicate, abortPredicate);
    }

    private static <T> Set<T> setPlusElems(Set<T> initial, T... newElement) {
        final HashSet<T> copy = new HashSet<>(initial);
        copy.addAll(Arrays.asList(newElement));
        return Collections.unmodifiableSet(copy);
    }

    public RetryPolicy withMaxRetries(int times) {
        return new RetryPolicy(times, retryOn, abortOn, retryPredicate, abortPredicate);
    }
    public boolean shouldContinue(RetryContext context) {
        if (tooManyRetries(context)) {
            return false;
        }
        if (abortPredicate.test(context.getLastThrowable())) {
            return false;
        }
        if (retryPredicate.test(context.getLastThrowable())) {
            return true;
        }
        return exceptionClassRetryable(context);
    }

    private boolean tooManyRetries(RetryContext context) {
        return context.getRetryCount() > maxRetries;
    }

    private static boolean matches(Class<? extends Throwable> throwable, Set<Class<? extends Throwable>> set) {
        return set.isEmpty() || set.stream().anyMatch(c -> c.isAssignableFrom(throwable));
    }

    private boolean exceptionClassRetryable(RetryContext context) {
        if (context.getLastThrowable() == null) {
            return false;
        }
        final Class<? extends Throwable> e = context.getLastThrowable().getClass();
        if (abortOn.isEmpty()) {
            return matches(e, retryOn);
        } else {
            return !matches(e, abortOn) && matches(e, retryOn);
        }
    }
}
