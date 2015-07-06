package io.cozmic.usher.core;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public interface MessageFilterFactory {
    MessageFilter createFilter(String pluginName, MessageMatcher messageMatcher, WriteStream<Buffer> writeStream);
}
