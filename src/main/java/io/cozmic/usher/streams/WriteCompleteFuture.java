package io.cozmic.usher.streams;

import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.NoStackTraceThrowable;

/**
 * Created by chuck on 10/23/15.
 */
public class WriteCompleteFuture<T> implements Future<T> {
    private final PipelinePack pipelinePack;

    public WriteCompleteFuture(PipelinePack pipelinePack) {
        this.pipelinePack = pipelinePack;
    }

    public PipelinePack getPipelinePack() {
        return pipelinePack;
    }
    private boolean failed;
    private boolean succeeded;
    private Handler<AsyncResult<T>> handler;
    private T result;
    private Throwable throwable;


    public static <T> WriteCompleteFuture<T> future(PipelinePack pack) {
        return new WriteCompleteFuture<>(pack);
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
        return succeeded;
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
    public boolean isComplete() {
        return failed || succeeded;
    }

    /**
     * Set a handler for the result. It will get called when it's complete
     */
    public void setHandler(Handler<AsyncResult<T>> handler) {
        this.handler = handler;
        checkCallHandler();
    }

    /**
     * Set the result. Any handler will be called, if there is one
     */
    public void complete(T result) {
        checkComplete();
        this.result = result;
        succeeded = true;
        checkCallHandler();
    }

    @Override
    public void complete() {
        complete(null);
    }

    /**
     * Set the failure. Any handler will be called, if there is one
     */
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

    private void checkCallHandler() {
        if (handler != null && isComplete()) {
            handler.handle(this);
        }
    }

    private void checkComplete() {
        if (succeeded || failed) {
            throw new IllegalStateException("Result is already complete: " + (succeeded ? "succeeded" : "failed"));
        }
    }
}
