package io.cozmic.usher.test;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.IOException;
import java.util.Objects;

/**
 * Created by chuck on 11/9/15.
 */
public class ErringDecoder implements DecoderPlugin {
    private JsonObject configObj;
    private Vertx vertx;
    private String onlyErrWhenMessageIs;

    @Override
    public void decode(PipelinePack pack, Handler<PipelinePack> pipelinePackHandler) throws IOException {

        final boolean onlyErrFlagIsOn = onlyErrWhenMessageIs != null;
        final Message message = pack.getMessage();
        final boolean errorModeTriggered = onlyErrFlagIsOn && Objects.equals(message.getPayload().toString(), onlyErrWhenMessageIs);

        if (errorModeTriggered) {
            throw new IOException("Could not decode");
        }

        pipelinePackHandler.handle(pack);

    }

    @Override
    public DecoderPlugin createNew() {
        final ErringDecoder erringDecoder = new ErringDecoder();
        try {
            erringDecoder.init(configObj, vertx);
        } catch (UsherInitializationFailedException e) {
            //do nothing, this is just a fake
        }
        return erringDecoder;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) throws UsherInitializationFailedException {

        this.configObj = configObj;
        this.vertx = vertx;

        onlyErrWhenMessageIs = configObj.getString("onlyErrWhenMessageIs");
    }
}
