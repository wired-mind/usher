package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.FilterPlugin;
import io.cozmic.usher.core.InPipeline;
import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.WriteStreamPool;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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
        final FilterStream filterStream = new FilterStream();
        messageStreamAsyncResultHandler.handle(Future.succeededFuture(new MessageStream(filterStream, filterStream)));
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }

    private class FilterStream implements InPipeline, OutPipeline {
        Logger logger = LoggerFactory.getLogger(FilterStream.class.getName());
        private Handler<Message> dataHandler;
        private boolean paused;
        private Handler<Void> drainHandler;
        private Handler<Throwable> exceptionHandler;

        @Override
        public FilterStream exceptionHandler(Handler<Throwable> exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        @Override
        public WriteStream<Message> write(Message message) {
            handleRequest(message, asyncResult -> {
                if (asyncResult.failed()) {
                    final Throwable cause = asyncResult.cause();
                    logger.error(cause.getMessage(), cause);
                    if (exceptionHandler != null) exceptionHandler.handle(cause);
                    return;
                }
                final Message response = asyncResult.result();
                dataHandler.handle(response);
            });
            return this;
        }

        @Override
        public WriteStream<Message> setWriteQueueMaxSize(int maxSize) {
            //no op
            return this;
        }

        @Override
        public boolean writeQueueFull() {
            return paused;
        }

        @Override
        public WriteStream<Message> drainHandler(Handler<Void> drainHandler) {
            this.drainHandler = drainHandler;
            return this;
        }

        @Override
        public ReadStream<Message> handler(Handler<Message> dataHandler) {
            this.dataHandler = dataHandler;
            return this;
        }

        @Override
        public ReadStream<Message> pause() {
            paused = true;
            return this;
        }

        @Override
        public ReadStream<Message> resume() {
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
        public ReadStream<Message> endHandler(Handler<Void> endHandler) {
            return this;
        }

        @Override
        public void stop(WriteStreamPool pool) {
            //no op
        }
    }

    public abstract void handleRequest(Message message, AsyncResultHandler<Message> asyncResultHandler);
}