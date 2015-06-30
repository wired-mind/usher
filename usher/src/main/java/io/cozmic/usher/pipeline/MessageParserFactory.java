package io.cozmic.usher.pipeline;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * Created by chuck on 6/29/15.
 */
public interface MessageParserFactory {
    MessageParser createParser(ReadStream<Buffer> readStream);
}
