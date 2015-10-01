package io.cozmic.usher.core;

import io.cozmic.usher.streams.ClosableWriteStream;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 7/9/15.
 */
public abstract class WriteStreamPool extends ObjectPool<ClosableWriteStream<Buffer>> {
    public WriteStreamPool(JsonObject configObj, Vertx vertx) {
        super(configObj, vertx);
    }
}
