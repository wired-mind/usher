package io.cozmic.usher.core;

import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 10/6/15.
 *
 * Rethrow is really just the default behavior so for now there's nothing special to do. We'll just return the same
 * pipeline instance.
 */
public class RethrowErrorStrategy implements ErrorStrategy {
    private final JsonObject config;

    public RethrowErrorStrategy(JsonObject config) {
        this.config = config;
    }

    @Override
    public OutPipeline wrap(OutPipeline outPipeline) {
        return outPipeline;
    }
}
