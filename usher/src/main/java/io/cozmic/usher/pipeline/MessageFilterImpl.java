package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.core.MessageFilter;
import io.cozmic.usher.core.MessageMatcher;
import io.cozmic.usher.message.Message;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public class MessageFilterImpl implements MessageFilter {
    private final WriteStream<Buffer> innerWriteStream;
    private final EncoderPlugin encoderPlugin;
    private final MessageMatcher messageMatcher;

    public MessageFilterImpl(WriteStream<Buffer> writeStream, EncoderPlugin encoderPlugin, MessageMatcher messageMatcher) {
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
    public WriteStream<Buffer> getInnerWriteStream() {
        return innerWriteStream;
    }
}
