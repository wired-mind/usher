package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.*;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;

/**
 * Default out pipeline filters using messageMatcher and then encodes the message to a buffer
 */
public class DefaultOutPipeline implements OutPipeline {
    private final WriteStream<Buffer> innerWriteStream;
    private final JsonObject config;
    private final EncoderPlugin encoderPlugin;
    private final FrameEncoderPlugin frameEncoderPlugin;
    private final MessageMatcher messageMatcher;

    public DefaultOutPipeline(WriteStream<Buffer> writeStream, JsonObject config, EncoderPlugin encoderPlugin, FrameEncoderPlugin frameEncoderPlugin, MessageMatcher messageMatcher) {
        this.innerWriteStream = writeStream;
        this.config = config;
        this.encoderPlugin = encoderPlugin;
        this.frameEncoderPlugin = frameEncoderPlugin;
        this.messageMatcher = messageMatcher;

        frameEncoderPlugin.setWriteHandler(innerWriteStream::write);
    }

    @Override
    public WriteStream<PipelinePack> exceptionHandler(Handler<Throwable> handler) {
        innerWriteStream.exceptionHandler(handler);
        return this;
    }

    @Override
    public WriteStream<PipelinePack> write(PipelinePack message) {
        if (messageMatcher.matches(message)) {
            encoderPlugin.encode(message, frameEncoderPlugin::encodeAndWrite);
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
}
