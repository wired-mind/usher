package io.cozmic.usher.test.localintegration;

import io.cozmic.usher.core.MessageInjector;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.AbstractFilter;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 9/30/15.
 */
public class InjectionFilter extends AbstractFilter {
    @Override
    public void handleRequest(PipelinePack pipelinePack, Future<Void> writeCompleteFuture, Handler<PipelinePack> dataHandler, MessageInjector messageInjector) {
        //echo first
        dataHandler.handle(pipelinePack);

        //now inject back into pipeline and only finish once the injection is done
        final Message message = pipelinePack.getMessage();
        final Buffer payload = message.getPayload();
        final PipelinePack injectedMessage = new PipelinePack(payload.toString());
        messageInjector.inject(injectedMessage, asyncResult -> {
            if (asyncResult.failed()) {
                writeCompleteFuture.fail(asyncResult.cause());
                return;
            }
            writeCompleteFuture.complete();
        });
    }
}
