package io.cozmic.usher.core;

import io.cozmic.usher.message.Message;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 7/3/15.
 */
public interface Router extends ReadStream<Message>, WriteStream<Message> {
    @Override
    Router exceptionHandler(Handler<Throwable> handler);
}
