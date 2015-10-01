package io.cozmic.usher.test.integration;

import io.cozmic.usher.core.MessageInjector;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 7/9/15.
 */
public class HelloWorldJsonFilter extends AbstractFilter {


    @Override
    public void handleRequest(PipelinePack pipelinePack, Future<Void> writeCompleteFuture, Handler<PipelinePack> dataHandler, MessageInjector messageInjector) {
        final JsonObject message = pipelinePack.getMessage();
        pipelinePack.setMessage(new JsonObject().put("hello", "world").put("original", message));
        dataHandler.handle(pipelinePack);
        writeCompleteFuture.complete();
    }
}
