package io.cozmic.usher.plugins.core;

import io.cozmic.usher.message.Message;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;

/**
 * Simplest example filter that does an Echo. Not implementing flow control.
 */
public class EchoFilter extends AbstractFilter {
    @Override
    public void handleRequest(Message message, AsyncResultHandler<Message> asyncResultHandler) {
        //would normally mutate the message or create a new response message
        asyncResultHandler.handle(Future.succeededFuture(message));
    }
}
