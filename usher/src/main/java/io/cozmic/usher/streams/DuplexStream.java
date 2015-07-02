package io.cozmic.usher.streams;

import io.cozmic.usher.message.Message;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public class DuplexStream<R, W> {
    private final ReadStream<R> readStream;
    private final WriteStream<W> writeStream;
    private Handler<Message> messageDecorator;

    public DuplexStream(ReadStream<R> readStream, WriteStream<W> writeStream) {
        this.readStream = readStream;
        this.writeStream = writeStream;
    }

    public DuplexStream(ReadStream<R> readStream, WriteStream<W> writeStream, Handler<Message> messageDecorator) {
        this(readStream, writeStream);
        this.messageDecorator = messageDecorator;
    }

    public WriteStream<W> getWriteStream() {
        return writeStream;
    }

    public ReadStream<R> getReadStream() {
        return readStream;
    }

    public ReadStream<R> pause() {
        return getReadStream().pause();
    }

    public ReadStream<R> resume() {
        return getReadStream().resume();
    }


    public void decorate(Message message, Handler<Message> decoratedHandler) {
        if (messageDecorator == null) {
            decoratedHandler.handle(message);
            return;
        }

//        messageDecorator.handle(m);
    }
}
