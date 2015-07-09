package io.cozmic.usher.core;

import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 6/29/15.
 */
public interface InPipelineFactory {
    InPipeline createDefaultInPipeline(String pluginName, DuplexStream<Buffer, Buffer> duplexStream);
}
