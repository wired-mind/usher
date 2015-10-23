package io.cozmic.usher.core;

import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.AsyncWriteStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 10/7/15.
 */
public class IgnoreErrorStrategy implements ErrorStrategy {
    static Logger logger = LoggerFactory.getLogger(IgnoreErrorStrategy.class.getName());
    private final JsonObject config;

    public IgnoreErrorStrategy(JsonObject config) {
        this.config = config;
    }

    @Override
    public OutPipeline wrap(OutPipeline outPipeline) {
        return new OutPipeline() {


            private Future<Void> writeCompleteFuture;

            @Override
            public void stop(WriteStreamPool pool) {
                outPipeline.stop(pool);
            }

            @Override
            public OutPipeline writeCompleteFuture(Future<Void> future) {
                writeCompleteFuture = future;
                return this;
            }

            @Override
            public void close() {
                outPipeline.close();
            }

            @Override
            public AsyncWriteStream<PipelinePack> write(PipelinePack data, Future<Void> future, PipelinePack context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public WriteStream<PipelinePack> exceptionHandler(Handler<Throwable> handler) {
                return this;
            }

            @Override
            public WriteStream<PipelinePack> write(PipelinePack data) {
                final Future<Void> ignoringFuture = Future.future();
                ignoringFuture.setHandler(asyncResult -> {
                    if (asyncResult.failed()) {
                        logger.warn("[IgnoreErrorStrategy] - Ignoring error.", asyncResult.cause());
                    }

                    writeCompleteFuture.complete();
                });

                outPipeline.write(data, ignoringFuture, data);
                return this;
            }

            @Override
            public WriteStream<PipelinePack> setWriteQueueMaxSize(int maxSize) {
                outPipeline.setWriteQueueMaxSize(maxSize);
                return this;
            }

            @Override
            public boolean writeQueueFull() {
                return outPipeline.writeQueueFull();
            }

            @Override
            public WriteStream<PipelinePack> drainHandler(Handler<Void> handler) {
                outPipeline.drainHandler(handler);
                return this;
            }
        };
    }
}
