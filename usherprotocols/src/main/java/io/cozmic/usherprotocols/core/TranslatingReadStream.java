package io.cozmic.usherprotocols.core;


import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * Created by chuck on 10/22/14.
 */
public interface TranslatingReadStream<T> extends ReadStream<T> {
    ReadStream<Buffer> translate(StreamProcessor streamProcessor);
}
