package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 7/2/15.
 */
public class CozmicDecoder implements DecoderPlugin {
    private JsonObject configObj;
    private Vertx vertx;

    @Override
    public void decode(PipelinePack pack, Handler<PipelinePack> messageHandler) {
//        Message message = pack.getMessage();
//        int pos = 0;
//        final int messageLength = pack.getInt(pos);
//        pos += 4;
//        final int messageIdLength = pack.getInt(pos);
//        pos += 4;
//        String messageId = pack.getString(pos, pos + messageIdLength);
//        pos += messageIdLength;
//
//        final Buffer body = pack.getBuffer(pos, pack.length());
//        messageHandler.handle(new Message(body));
    }



    @Override
    public DecoderPlugin createNew() {
        final CozmicDecoder cozmicDecoder = new CozmicDecoder();
        cozmicDecoder.init(configObj, vertx);
        return cozmicDecoder;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }
}
