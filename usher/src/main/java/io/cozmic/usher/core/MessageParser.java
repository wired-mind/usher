package io.cozmic.usher.core;

import io.cozmic.usher.message.Message;
import io.vertx.core.streams.ReadStream;

/**
 * Created by chuck on 6/30/15.
 */
public interface MessageParser extends ReadStream<Message> {
}
