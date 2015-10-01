package io.cozmic.usher.core;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public interface OutPipelineFactory {
    OutPipeline createDefaultOutPipeline(String pluginName, JsonObject config, WriteStream<Buffer> writeStream);
}
