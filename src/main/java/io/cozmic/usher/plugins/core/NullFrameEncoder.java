package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.FrameEncoderPlugin;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 7/10/15.
 */
public class NullFrameEncoder implements FrameEncoderPlugin{

    private JsonObject configObj;
    private Vertx vertx;


    @Override
    public void encode(Buffer buffer, Handler<Buffer> doneHandler) {
        doneHandler.handle(buffer);
    }

    @Override
    public FrameEncoderPlugin createNew() {
        final NullFrameEncoder nullFrameEncoder = new NullFrameEncoder();
        nullFrameEncoder.init(configObj, vertx);
        return nullFrameEncoder;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }
}
