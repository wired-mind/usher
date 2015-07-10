package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 7/9/15.
 */
public class PayloadEncoder implements EncoderPlugin {
    private JsonObject configObj;
    private Vertx vertx;

    @Override
    public void encode(PipelinePack pipelinePack, Handler<Buffer> bufferHandler) {
        final Message message = pipelinePack.getMessage();
        bufferHandler.handle(Buffer.buffer(message.getPayload()));
    }

    @Override
    public EncoderPlugin createNew() {
        return this;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }
}
