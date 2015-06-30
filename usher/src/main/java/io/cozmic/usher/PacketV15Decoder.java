package io.cozmic.usher;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usherprotocols.core.Message;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 6/29/15.
 */
public class PacketV15Decoder implements DecoderPlugin {
    @Override
    public void init(JsonObject configObj, Vertx vertx) {

    }


    @Override
    public void decode(Buffer record, Handler<Message> messageHandler) {

    }
}
