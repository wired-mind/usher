package io.cozmic.usher.core;


import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.NoStackTraceThrowable;

/**
 * Created by chuck on 10/24/14.
 */
public class CountDownFutureResult<T> implements Future<T> {
    private final int size;
    private boolean failed;
    private int succeededCount;
    private Handler<AsyncResult<T>> handler;
    private T result;
    private Throwable throwable;


    public static CountDownFutureResult<Void> dynamicStarter(int size) {
        CountDownFutureResult<Void> starter = new CountDownFutureResult<>(size);
        return starter;
    }

    public CountDownFutureResult(int size) {
        this.size = size;
    }
    /**
     * The result of the operation. This will be null if the operation failed.
     */
    public T result() {
        return result;
    }

    /**
     * An exception describing failure. This will be null if the operation succeeded.
     */
    public Throwable cause() {
        return throwable;
    }

    /**
     * Did it succeeed?
     */
    public boolean succeeded() {
        return succeededCount == size;
    }

    /**
     * Did it fail?
     */
    public boolean failed() {
        return failed;
    }

    /**
     * Has it completed?
     */
    @Override
    public boolean isComplete() {
        return failed || succeeded();
    }

    @Override
    public void complete(T result) {
        checkComplete();
        this.result = result;
        succeededCount++;
        checkCallHandler();
    }

    @Override
    public void complete() {
       complete(null);
    }

    @Override
    public void fail(Throwable throwable) {
        checkComplete();
        this.throwable = throwable;
        failed = true;
        checkCallHandler();
    }

    @Override
    public void fail(String failureMessage) {
        fail(new NoStackTraceThrowable(failureMessage));
    }

    /**
     * Set a handler for the result. It will get called when it's complete
     */
    public void setHandler(Handler<AsyncResult<T>> handler) {
        this.handler = handler;
        checkCallHandler();
    }



    private void checkCallHandler() {
        if (handler != null && isComplete()) {
            handler.handle(this);
        }
    }

    private void checkComplete() {
        if (succeeded() || failed) {
            throw new IllegalStateException("Result is already complete: " + (succeeded() ? "succeeded" : "failed"));
        }
    }
}
