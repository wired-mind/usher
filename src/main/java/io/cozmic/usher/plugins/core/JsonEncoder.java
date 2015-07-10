package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 7/9/15.
 */
public class JsonEncoder implements EncoderPlugin {
    private JsonObject configObj;
    private Vertx vertx;

    @Override
    public void encode(PipelinePack pipelinePack, Handler<Buffer> bufferHandler) {
        final JsonObject message = pipelinePack.getMessage();
        final Buffer buffer = Buffer.buffer(message.toString());

        bufferHandler.handle(buffer);
    }

    @Override
    public EncoderPlugin createNew() {
        final JsonEncoder jsonEncoder = new JsonEncoder();
        jsonEncoder.init(configObj, vertx);
        return jsonEncoder;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }
}
