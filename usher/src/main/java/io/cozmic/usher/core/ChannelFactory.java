package io.cozmic.usher.core;

import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;

/**
 * Created by chuck on 7/6/15.
 */
public interface ChannelFactory {
    void createFullDuplexMuxChannel(MessageStream inputMessageStream);
}
