package io.cozmic.usher.core;

import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.IOException;

/**
 * Created by chuck on 6/25/15.
 */
public interface Plugin {
    void init(JsonObject configObj, Vertx vertx) throws UsherInitializationFailedException;
}
