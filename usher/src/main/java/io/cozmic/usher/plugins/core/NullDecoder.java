package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usher.message.Message;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 6/30/15.
 */
public class NullDecoder implements DecoderPlugin {
    @Override
    public void decode(Buffer record, Handler<Message> messageHandler) {
        messageHandler.handle(new Message(record));
    }

    @Override
    public DecoderPlugin createNew() {
        return this;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

    }
}
