package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.SplitterPlugin;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 6/30/15.
 */
public class NullSplitter implements SplitterPlugin {
    @Override
    public void findRecord(Buffer buffer, Handler<Buffer> bufferHandler) {
        bufferHandler.handle(buffer);
    }

    @Override
    public SplitterPlugin createNew() {
        return this;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

    }
}
