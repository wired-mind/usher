package io.cozmic.usher.core;

import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.ClosableWriteStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 6/30/15.
 */
public interface OutPipeline extends ClosableWriteStream<PipelinePack> {

    void stop(WriteStreamPool pool);

    OutPipeline writeCompleteHandler(Handler<AsyncResult<Void>> handler);
}
