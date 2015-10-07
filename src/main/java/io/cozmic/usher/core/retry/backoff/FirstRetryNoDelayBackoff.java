package io.cozmic.usher.core.retry.backoff;


import io.cozmic.usher.core.retry.AsyncRetryContext;
import io.cozmic.usher.core.retry.RetryContext;

public class FirstRetryNoDelayBackoff extends BackoffWrapper {

    public FirstRetryNoDelayBackoff(Backoff target) {
        super(target);
    }

    @Override
    public long delayMillis(RetryContext context) {
        if (context.isFirstRetry()) {
            return 0;
        } else {
            return target.delayMillis(decrementRetryCount(context));
        }
    }

    private RetryContext decrementRetryCount(RetryContext context) {
        return ((AsyncRetryContext) context).prevRetry();
    }

}
