package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.FilterPlugin;
import io.cozmic.usher.core.InPipeline;
import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.WriteStreamPool;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 7/9/15.
 */
public abstract class AbstractFilter implements FilterPlugin {


    private JsonObject configObj;
    private Vertx vertx;

    @Override
    public void run(AsyncResultHandler<MessageStream> messageStreamAsyncResultHandler) {
        start(result -> {
            if (result.failed()) {
                messageStreamAsyncResultHandler.handle(Future.failedFuture(result.cause()));
                return;
            }

            final FilterStream filterStream = new FilterStream();
            messageStreamAsyncResultHandler.handle(Future.succeededFuture(new MessageStream(filterStream, filterStream)));
        });
    }

    protected abstract void start(AsyncResultHandler<Void> resultHandler);

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }

    public JsonObject getConfigObj() {
        return configObj;
    }

    public Vertx getVertx() {
        return vertx;
    }

    private class FilterStream implements InPipeline, OutPipeline {
        Logger logger = LoggerFactory.getLogger(FilterStream.class.getName());
        private Handler<PipelinePack> dataHandler;
        private boolean paused;
        private Handler<Void> drainHandler;
        private Handler<Throwable> exceptionHandler;

        @Override
        public FilterStream exceptionHandler(Handler<Throwable> exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        @Override
        public WriteStream<PipelinePack> write(PipelinePack pipelinePack) {
            handleRequest(pipelinePack, asyncResult -> {
                if (asyncResult.failed()) {
                    final Throwable cause = asyncResult.cause();
                    logger.error(cause.getMessage(), cause);
                    if (exceptionHandler != null) exceptionHandler.handle(cause);
                    return;
                }
                final PipelinePack response = asyncResult.result();
                dataHandler.handle(response);
            });
            return this;
        }

        @Override
        public WriteStream<PipelinePack> setWriteQueueMaxSize(int maxSize) {
            //no op
            return this;
        }

        @Override
        public boolean writeQueueFull() {
            return paused;
        }

        @Override
        public WriteStream<PipelinePack> drainHandler(Handler<Void> drainHandler) {
            this.drainHandler = drainHandler;
            return this;
        }

        @Override
        public ReadStream<PipelinePack> handler(Handler<PipelinePack> dataHandler) {
            this.dataHandler = dataHandler;
            return this;
        }

        @Override
        public ReadStream<PipelinePack> pause() {
            paused = true;
            return this;
        }

        @Override
        public ReadStream<PipelinePack> resume() {
            paused = false;
            vertx.runOnContext(v -> callDrainHandler());
            return this;
        }

        private synchronized void callDrainHandler() {
            if (drainHandler != null) {
                if (!writeQueueFull()) {
                    drainHandler.handle(null);
                }
            }
        }

        @Override
        public ReadStream<PipelinePack> endHandler(Handler<Void> endHandler) {
            return this;
        }

        @Override
        public void stop(WriteStreamPool pool) {
            //no op
        }
    }

    public abstract void handleRequest(PipelinePack pipelinePack, AsyncResultHandler<PipelinePack> asyncResultHandler);
}