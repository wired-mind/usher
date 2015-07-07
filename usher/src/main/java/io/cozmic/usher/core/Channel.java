package io.cozmic.usher.core;

import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;

import java.util.List;

/**
 * Created by chuck on 6/30/15.
 */
public interface Channel {
    Channel start();

    Channel stop();


    Channel endHandler(Handler<Void> endHandler);

    List<MessageParser> getResponseStreams();

    void init(AsyncResultHandler<Channel> readyHandler);

    void init(MessageStream optionalMessageStream, AsyncResultHandler<Channel> readyHandler);
}
