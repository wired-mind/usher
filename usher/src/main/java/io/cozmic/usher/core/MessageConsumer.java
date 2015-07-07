package io.cozmic.usher.core;

import io.cozmic.usher.message.Message;
import io.vertx.core.streams.ReadStream;

/**
 * Created by chuck on 7/6/15.
 */
public interface MessageConsumer extends ReadStream<Message> {
    MessageParser getResponseStream();

    void unregister();
}
