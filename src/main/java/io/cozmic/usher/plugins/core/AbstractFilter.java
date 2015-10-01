package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.*;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import java.util.Objects;

/**
 * Created by chuck on 7/9/15.
 */
public abstract class AbstractFilter implements FilterPlugin {


    private JsonObject configObj;
    private Vertx vertx;

    @Override
    public void run(MessageInjector messageInjector, AsyncResultHandler<MessageStream> messageStreamAsyncResultHandler) {
        Future<Void> startFuture = Future.future();
        startFuture.setHandler(result -> {
            if (result.failed()) {
                messageStreamAsyncResultHandler.handle(Future.failedFuture(result.cause()));
                return;
            }

            final FilterStream filterStream = new FilterStream(messageInjector);
            messageStreamAsyncResultHandler.handle(Future.succeededFuture(new MessageStream(filterStream, filterStream)));
        });
        start(startFuture);
    }

    public void start() {

    }
    public void start(Future<Void> startFuture) {
        start();
        startFuture.complete();
    };

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
        private final MessageInjector messageInjector;
        Logger logger = LoggerFactory.getLogger(FilterStream.class.getName());
        private Handler<PipelinePack> dataHandler;
        private boolean paused;
        private Handler<Void> drainHandler;
        private Handler<Throwable> exceptionHandler;
        private Handler<AsyncResult<Void>> writeCompleteHandler;

        public FilterStream(MessageInjector messageInjector) {
            Objects.requireNonNull(messageInjector, "MessageInjector is required");
            this.messageInjector = messageInjector;
        }

        @Override
        public void close() {
            //no op
        }

        @Override
        public FilterStream exceptionHandler(Handler<Throwable> exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        @Override
        public WriteStream<PipelinePack> write(PipelinePack pipelinePack) {
            Objects.requireNonNull(writeCompleteHandler, "writeCompleteHandler required. This should be set automatically by the MuxRegistration.");
            Objects.requireNonNull(dataHandler, "dataHandler required. This should be set automatically by the MuxRegistration.");

            try {
                handleRequest(pipelinePack, writeDoneFuture(), dataHandler, messageInjector);
            } catch (Throwable throwable) {
                if (exceptionHandler != null) exceptionHandler.handle(throwable);
            }
            return this;
        }

        private Future<Void> writeDoneFuture() {
            final Future<Void> doneFuture = Future.future();
            doneFuture.setHandler(writeCompleteHandler);
            return doneFuture;
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

        @Override
        public OutPipeline writeCompleteHandler(Handler<AsyncResult<Void>> handler) {
            writeCompleteHandler = handler;
            return this;
        }
    }

    public abstract void handleRequest(PipelinePack pipelinePack, Future<Void> writeCompleteFuture, Handler<PipelinePack> dataHandler, MessageInjector messageInjector);
}