package io.cozmic.usher.message;

import de.odysseus.el.util.SimpleContext;
import io.cozmic.usher.core.retry.AsyncRetryContext;
import io.cozmic.usher.core.retry.RetryContext;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

import javax.el.ExpressionFactory;

/**
 * Created by chuck on 7/9/15.
 */
public class PipelinePack {
    private static ExpressionFactory factory = ExpressionFactory.newInstance();
    private Object message;
    private Buffer msgBytes;
    private RetryContext retryContext;
    private SimpleContext runtimeContext;

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

    /**
     * Lazily return the JUEL runtime context
     * @return
     */
    public SimpleContext getRuntimeContext() {
        if (runtimeContext == null) {
            runtimeContext = new SimpleContext();
            factory.createValueExpression(runtimeContext, "${pack}", PipelinePack.class).setValue(runtimeContext, this);
        }
        return runtimeContext;
    }
}
