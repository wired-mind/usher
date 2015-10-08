package io.cozmic.usher.message;

import io.cozmic.usher.core.retry.AsyncRetryContext;
import io.cozmic.usher.core.retry.RetryContext;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 7/9/15.
 */
public class PipelinePack {
    private Object message;
    private Buffer msgBytes;
    private RetryContext retryContext;

    public PipelinePack(Buffer msgBytes) {

        this.msgBytes = msgBytes;
    }

    public PipelinePack() {
        this.message = new Message();
    }

    public PipelinePack(Object message) {
        this.message = message;
    }

    public <T> T getMessage() {
        return (T) message;
    }


    public Buffer getMsgBytes() {
        return msgBytes;
    }

    public void setMessage(Object message) {
        this.message = message;
    }

    public void setRetryContext(RetryContext retryContext) {
        this.retryContext = retryContext;
    }

    public RetryContext getRetryContext() {
        return retryContext;
    }
}
