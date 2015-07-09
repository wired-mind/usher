package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.message.Message;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * Default out pipeline filters using messageMatcher and then encodes the message to a buffer
 */
public class DefaultOutPipeline implements OutPipeline {
    private final WriteStream<Buffer> innerWriteStream;
    private final EncoderPlugin encoderPlugin;
    private final MessageMatcher messageMatcher;

    public DefaultOutPipeline(WriteStream<Buffer> writeStream, EncoderPlugin encoderPlugin, MessageMatcher messageMatcher) {
        this.innerWriteStream = writeStream;
        this.encoderPlugin = encoderPlugin;
        this.messageMatcher = messageMatcher;
    }

    @Override
    public WriteStream<Message> exceptionHandler(Handler<Throwable> handler) {
        innerWriteStream.exceptionHandler(handler);
        return this;
    }

    @Override
    public WriteStream<Message> write(Message message) {
        if (messageMatcher.matches(message)) {
            encoderPlugin.encode(message, innerWriteStream::write);
        }
        return this;
    }


    @Override
    public WriteStream<Message> setWriteQueueMaxSize(int maxSize) {
        innerWriteStream.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return innerWriteStream.writeQueueFull();
    }

    @Override
    public WriteStream<Message> drainHandler(Handler<Void> handler) {
        innerWriteStream.drainHandler(handler);
        return this;
    }


    @Override
    public void stop(WriteStreamPool pool) {
        pool.returnObject(innerWriteStream);
    }
}
