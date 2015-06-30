package io.cozmic.usher.pipeline;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public interface MessageFilterFactory {
    MessageFilter createFilter(WriteStream<Buffer> writeStream);
}
