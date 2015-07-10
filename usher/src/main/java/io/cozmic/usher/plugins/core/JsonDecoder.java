package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 7/9/15.
 */
public class JsonDecoder implements DecoderPlugin {
    private JsonObject configObj;
    private Vertx vertx;

    @Override
    public void decode(PipelinePack pack, Handler<PipelinePack> pipelinePackHandler) {
        final Message message = pack.getMessage();
        final JsonObject jsonObject = new JsonObject(message.getPayload());
        pack.setMessage(jsonObject);

        pipelinePackHandler.handle(pack);
    }

    @Override
    public DecoderPlugin createNew() {
        final JsonDecoder jsonDecoder = new JsonDecoder();
        jsonDecoder.init(configObj, vertx);
        return jsonDecoder;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }
}
