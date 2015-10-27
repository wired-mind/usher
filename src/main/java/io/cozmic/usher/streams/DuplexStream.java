package io.cozmic.usher.streams;

import io.cozmic.usher.core.ErrorStrategy;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public class DuplexStream<R, W> {
    private final ReadStream<R> readStream;
    private final ClosableWriteStream<W> writeStream;

    private Handler<PipelinePack> packDecorator;
    private Handler<Void> closeHandler;
    private Handler<WriteCompleteFuture> writeCompleteHandler;


    public DuplexStream(ReadStream<R> readStream, ClosableWriteStream<W> writeStream) {
        this.readStream = readStream;
        this.writeStream = writeStream;
    }

    public DuplexStream(ReadStream<R> readStream, ClosableWriteStream<W> writeStream, Handler<PipelinePack> packDecorator) {
        this(readStream, writeStream, packDecorator, null);
    }

    public DuplexStream(ReadStream<R> readStream, ClosableWriteStream<W> writeStream, Handler<PipelinePack> packDecorator, Handler<Void> closeHandler) {
        this(readStream, writeStream);
        this.packDecorator = packDecorator;
        this.closeHandler = closeHandler;
    }

    public ClosableWriteStream<W> getWriteStream() {
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


    public void decorate(PipelinePack pack, Handler<PipelinePack> decoratedHandler) {
        if (packDecorator == null) {
            decoratedHandler.handle(pack);
            return;
        }

        packDecorator.handle(pack);
        decoratedHandler.handle(pack);
    }

    public void close() {
        if (closeHandler != null) closeHandler.handle(null);
    }


    public Handler<WriteCompleteFuture> getWriteCompleteHandler() {
        return writeCompleteHandler;
    }

    public void writeCompleteHandler(Handler<WriteCompleteFuture> writeCompleteHandler) {
        this.writeCompleteHandler = writeCompleteHandler;
    }

    public DuplexStream<R, W> closeHandler(Handler<Void> handler) {
        this.closeHandler = handler;
        return this;
    }

    public DuplexStream<R, W> packDecorator(Handler<PipelinePack> decorator) {
        this.packDecorator = decorator;
        return this;
    }



}
