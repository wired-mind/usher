package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * The NullEncoder assumes that the Message is a Buffer
 */
public class NullEncoder implements EncoderPlugin {
    @Override
    public void encode(PipelinePack pipelinePack, Handler<Buffer> bufferHandler) {
        Buffer answer = pipelinePack.getMsgBytes();
        final Object message = pipelinePack.getMessage();
        if (message instanceof Buffer) {
            answer = (Buffer) message;
        }
        bufferHandler.handle(answer);
    }

    @Override
    public EncoderPlugin createNew() {
        return this;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

    }
}
