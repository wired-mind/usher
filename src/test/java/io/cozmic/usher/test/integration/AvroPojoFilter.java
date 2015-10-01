package io.cozmic.usher.test.integration;

import io.cozmic.usher.core.MessageInjector;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.cozmic.usher.test.Pojo;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Filter 'User' GenericRecord.
 * <p>
 * Created by Craig Earley on 8/19/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class AvroPojoFilter extends AbstractFilter {


    @Override
    public void handleRequest(PipelinePack pipelinePack, Future<Void> writeCompleteFuture, Handler<PipelinePack> dataHandler, MessageInjector messageInjector) {
        final Pojo pojo = pipelinePack.getMessage();

        // Set favorite color to green
        pojo.setFavoriteColor("green");
        pojo.setName("changed");

        pipelinePack.setMessage(pojo);
        dataHandler.handle(pipelinePack);
        writeCompleteFuture.complete();
    }
}
