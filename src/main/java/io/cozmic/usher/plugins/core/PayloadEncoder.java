package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.apache.commons.codec.binary.Hex;

/**
 * Created by chuck on 7/9/15.
 */
public class PayloadEncoder implements EncoderPlugin {
    private JsonObject configObj;
    private Vertx vertx;
    private boolean hex;

    @Override
    public void encode(PipelinePack pipelinePack, Handler<Buffer> bufferHandler) {
        final Message message = pipelinePack.getMessage();


        if (hex) {
            final char[] chars = Hex.encodeHex(message.getPayload().getBytes());
            bufferHandler.handle(Buffer.buffer(new String(chars)));
            return;
        }

        bufferHandler.handle(message.getPayload());
    }

    @Override
    public EncoderPlugin createNew() {
        return this;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;

        this.hex = configObj.getBoolean("hex", false);
    }
}
