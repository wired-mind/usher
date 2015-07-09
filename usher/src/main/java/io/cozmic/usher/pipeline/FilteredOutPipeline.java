package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.MessageMatcher;
import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.WriteStreamPool;
import io.cozmic.usher.message.Message;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 7/9/15.
 */
public class FilteredOutPipeline implements OutPipeline {
    private final OutPipeline outPipeline;
    private final MessageMatcher messageMatcher;

    public FilteredOutPipeline(OutPipeline outPipeline, MessageMatcher messageMatcher) {
        this.outPipeline = outPipeline;
        this.messageMatcher = messageMatcher;
    }

    @Override
    public void stop(WriteStreamPool pool) {
        outPipeline.stop(pool);
    }

    @Override
    public WriteStream<Message> exceptionHandler(Handler<Throwable> exceptionHandler) {
        outPipeline.exceptionHandler(exceptionHandler);
        return this;
    }

    @Override
    public WriteStream<Message> write(Message message) {
        if (messageMatcher.matches(message)) {
            outPipeline.write(message);
        }
        return this;
    }

    @Override
    public WriteStream<Message> setWriteQueueMaxSize(int maxSize) {
        outPipeline.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return outPipeline.writeQueueFull();
    }

    @Override
    public WriteStream<Message> drainHandler(Handler<Void> handler) {
        outPipeline.drainHandler(handler);
        return this;
    }
}
