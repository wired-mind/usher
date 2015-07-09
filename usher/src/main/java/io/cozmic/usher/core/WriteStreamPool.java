package io.cozmic.usher.core;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 7/9/15.
 */
public abstract class WriteStreamPool extends ObjectPool<WriteStream<Buffer>> {
    public WriteStreamPool(JsonObject configObj, Vertx vertx) {
        super(configObj, vertx);
    }
}
