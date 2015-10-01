package io.cozmic.usher.core;

import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 7/6/15.
 */
public interface MuxRegistration extends ReadStream<PipelinePack>, WriteStream<PipelinePack> {

    void unregister();

    @Override
    MuxRegistration exceptionHandler(Handler<Throwable> exceptionHandler);

    boolean matches(PipelinePack pack);


}
