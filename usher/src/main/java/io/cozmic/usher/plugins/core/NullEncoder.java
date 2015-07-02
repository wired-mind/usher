package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.message.Message;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 6/30/15.
 */
public class NullEncoder implements EncoderPlugin {
    @Override
    public void encode(Message message, Handler<Buffer> bufferHandler) {
        bufferHandler.handle(message.getBody());
    }

    @Override
    public EncoderPlugin createNew() {
        return this;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

    }
}
