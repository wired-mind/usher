package io.cozmic.usher.core;

import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public interface OutPipeline extends WriteStream<PipelinePack> {

    void stop(WriteStreamPool pool);

    OutPipeline writeCompleteHandler(Handler<AsyncResult<Void>> handler);
}
