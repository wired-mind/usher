package io.cozmic.usher.core;

import io.cozmic.usher.streams.ClosableWriteStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 6/30/15.
 */
public interface OutPipelineFactory {
    OutPipeline createDefaultOutPipeline(String pluginName, JsonObject config, ClosableWriteStream<Buffer> writeStream);
}
