package io.cozmic.usherprotocols.core;

import org.vertx.java.core.Handler;
import org.vertx.java.core.streams.ReadStream;

/**
 * Created by chuck on 10/2/14.
 */
public interface MessageReadStream<T> extends ReadStream<T> {
    T messageHandler(Handler<Message> messageHandler);
}
