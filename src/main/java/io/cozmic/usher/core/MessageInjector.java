package io.cozmic.usher.core;

import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Created by chuck on 9/30/15.
 */
public interface MessageInjector {
    void inject(PipelinePack pipelinePack, Handler<AsyncResult<PipelinePack>> doneHandler);
}
