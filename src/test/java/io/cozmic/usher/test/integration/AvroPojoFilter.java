package io.cozmic.usher.test.integration;

import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.cozmic.usher.test.Pojo;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;

/**
 * Filter 'User' GenericRecord.
 * <p>
 * Created by Craig Earley on 8/19/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class AvroPojoFilter extends AbstractFilter {

    @Override
    protected void start(AsyncResultHandler<Void> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void handleRequest(PipelinePack pipelinePack, AsyncResultHandler<PipelinePack> asyncResultHandler) {
        final Pojo pojo = pipelinePack.getMessage();

        // Set favorite color to green
        pojo.setFavoriteColor("green");

        pipelinePack.setMessage(pojo);
        asyncResultHandler.handle(Future.succeededFuture(pipelinePack));
    }
}
