package io.cozmic.usher.core;

import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 10/6/15.
 */
public interface ErrorStrategy {
    JsonObject DEFAULT_ERROR_STRATEGY = new JsonObject().put("type", "rethrow");

    static ErrorStrategy build(Vertx vertx, JsonObject errorStrategy) throws UsherInitializationFailedException {
        final String type = errorStrategy.getString("type");
        switch (type) {
            case "rethrow":
                return new RethrowErrorStrategy(errorStrategy);
            case "ignore":
                return new IgnoreErrorStrategy(errorStrategy);
            case "retry":
                return new RetryErrorStrategy(vertx, errorStrategy);
            default:
                throw new UsherInitializationFailedException("Unknown error strategy");
        }
    }

    OutPipeline wrap(OutPipeline outPipeline);
}
