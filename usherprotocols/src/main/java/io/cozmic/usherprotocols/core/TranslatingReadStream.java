package io.cozmic.usherprotocols.core;

import org.vertx.java.core.streams.ReadStream;

/**
 * Created by chuck on 10/22/14.
 */
public interface TranslatingReadStream<T> extends ReadStream<T> {
    ReadStream<?> translate(StreamProcessor streamProcessor);
}
