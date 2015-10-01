package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.ClosableWriteStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;

import java.io.IOException;

/**
 * Default out pipeline filters using messageMatcher and then encodes the message to a buffer
 */
public class DefaultOutPipeline implements OutPipeline {
    private final ClosableWriteStream<Buffer> innerWriteStream;
    private final JsonObject config;
    private final EncoderPlugin encoderPlugin;
    private final FrameEncoderPlugin frameEncoderPlugin;
    private Handler<Throwable> exceptionHandler;
    private Handler<AsyncResult<Void>> writeCompleteHandler;

    public DefaultOutPipeline(ClosableWriteStream<Buffer> writeStream, JsonObject config, EncoderPlugin encoderPlugin, FrameEncoderPlugin frameEncoderPlugin) {
        this.innerWriteStream = writeStream;
        this.config = config;
        this.encoderPlugin = encoderPlugin;
        this.frameEncoderPlugin = frameEncoderPlugin;


    }

    @Override
    public WriteStream<PipelinePack> exceptionHandler(Handler<Throwable> handler) {
        exceptionHandler = handler;
        innerWriteStream.exceptionHandler(handler);
        return this;
    }

    @Override
    public WriteStream<PipelinePack> write(PipelinePack pack) {
        try {
            encoderPlugin.encode(pack, encoded -> {
                frameEncoderPlugin.encode(encoded, framed -> {
                    innerWriteStream.write(framed, writeCompleteHandler);
                });
            });
        } catch (IOException e) {
            if (exceptionHandler != null) exceptionHandler.handle(e);
        }

        return this;
    }


    @Override
    public WriteStream<PipelinePack> setWriteQueueMaxSize(int maxSize) {
        innerWriteStream.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return innerWriteStream.writeQueueFull();
    }

    @Override
    public WriteStream<PipelinePack> drainHandler(Handler<Void> handler) {
        innerWriteStream.drainHandler(handler);
        return this;
    }


    @Override
    public void stop(WriteStreamPool pool) {
        pool.returnObject(innerWriteStream);
    }

    @Override
    public OutPipeline writeCompleteHandler(Handler<AsyncResult<Void>> handler) {
        writeCompleteHandler = handler;
        return this;
    }

    @Override
    public OutPipeline write(PipelinePack data, Handler<AsyncResult<Void>> writeCompleteHandler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }
}
