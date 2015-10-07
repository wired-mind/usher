package io.cozmic.usher.core.retry;


public interface RetryContext {
    boolean willRetry();

    /**
     * Which retry is being executed right now
     * @return 1 means it's the first retry, i.e. action is executed for the second time
     */
    int getRetryCount();

    Throwable getLastThrowable();

    default boolean isFirstRetry() {
        return getRetryCount() == 1;
    }

}
