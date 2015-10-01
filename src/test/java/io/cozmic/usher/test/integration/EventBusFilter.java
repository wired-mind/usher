package io.cozmic.usher.test.integration;

import io.cozmic.usher.core.MessageInjector;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * The EventBusFilter puts messages
 * on the vertx event bus.
 * <p>
 * Created by Craig Earley on 8/27/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class EventBusFilter extends AbstractFilter {
    private static final Logger logger = LoggerFactory.getLogger(EventBusFilter.class.getName());
    public static final String EVENT_BUS_ADDRESS = EventBusFilter.class.getName();



    @Override
    public void handleRequest(PipelinePack pipelinePack, Future<Void> writeCompleteFuture, Handler<PipelinePack> dataHandler, MessageInjector messageInjector) {
        Object obj = pipelinePack.getMessage();
        if (obj instanceof Message) {
            obj = ((Message)obj).getPayload().toString();
        }
        this.getVertx().eventBus().send(EVENT_BUS_ADDRESS, obj.hashCode());
        logger.info("hash" + obj.hashCode());
        writeCompleteFuture.complete();
    }
}
