package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.core.MessageFilter;
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

    public MessageFilterImpl(WriteStream<Buffer> writeStream, EncoderPlugin encoderPlugin) {
        this.innerWriteStream = writeStream;
        this.encoderPlugin = encoderPlugin;
    }

    @Override
    public WriteStream<Message> exceptionHandler(Handler<Throwable> handler) {
        innerWriteStream.exceptionHandler(handler);
        return this;
    }

    @Override
    public WriteStream<Message> write(Message message) {
        if (matches(message)) {
            encoderPlugin.encode(message, innerWriteStream::write);
        }
        return this;
    }

    //TODO: return true until we implement filter logic
    private boolean matches(Message message) {
        return true;
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
