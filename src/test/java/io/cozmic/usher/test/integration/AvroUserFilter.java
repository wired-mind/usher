package io.cozmic.usher.test.integration;

import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import org.apache.avro.generic.GenericRecord;

/**
 * Filter 'User' GenericRecord.
 * <p>
 * Created by Craig Earley on 8/19/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class AvroUserFilter extends AbstractFilter {

    @Override
    protected void start(AsyncResultHandler<Void> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void handleRequest(PipelinePack pipelinePack, AsyncResultHandler<PipelinePack> asyncResultHandler) {
        final GenericRecord user = pipelinePack.getMessage();

        // Set favorite color to green
        user.put("favorite_color", "green");

        pipelinePack.setMessage(user);
        asyncResultHandler.handle(Future.succeededFuture(pipelinePack));
    }
}
