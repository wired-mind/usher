package io.cozmic.usher.streams;

import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Created by chuck on 10/22/15.
 */
public interface WriteContext extends Handler<AsyncResult<Void>> {
    public PipelinePack getPack();
}
