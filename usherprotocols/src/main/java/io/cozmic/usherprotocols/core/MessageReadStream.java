package io.cozmic.usherprotocols.core;


import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * Created by chuck on 10/2/14.
 */
public interface MessageReadStream extends ReadStream<Buffer> {
    MessageReadStream messageHandler(Handler<Message> messageHandler);
}
