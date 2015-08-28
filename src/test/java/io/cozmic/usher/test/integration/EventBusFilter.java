package io.cozmic.usher.test.integration;

import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;

/**
 * The EventBusFilter puts messages
 * on the vertx event bus.
 * <p>
 * Created by Craig Earley on 8/27/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class EventBusFilter extends AbstractFilter {

    public static final String EVENT_BUS_ADDRESS = EventBusFilter.class.getName();

    @Override
    protected void start(AsyncResultHandler<Void> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void handleRequest(PipelinePack pipelinePack, AsyncResultHandler<PipelinePack> asyncResultHandler) {
        Message message = pipelinePack.getMessage();
        this.getVertx().eventBus().send(EVENT_BUS_ADDRESS, message.getPayload().getBytes());
        asyncResultHandler.handle(Future.succeededFuture());
    }
}
