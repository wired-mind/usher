package io.cozmic.usher.pipeline;

import io.cozmic.usherprotocols.core.Message;
import io.vertx.core.streams.ReadStream;

/**
 * Created by chuck on 6/30/15.
 */
public interface MessageParser extends ReadStream<Message> {
}
