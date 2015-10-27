package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.*;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.AsyncWriteStream;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by chuck on 7/9/15.
 */
public abstract class AbstractFilter implements FilterPlugin {


    public static final int DEFAULT_TIMEOUT = -1;   //defaults to -1 which means no timeout
    private JsonObject configObj;
    private Vertx vertx;
    private Integer timeout = DEFAULT_TIMEOUT;

    @Override
    public void run(MessageInjector messageInjector, AsyncResultHandler<MessageStream> messageStreamAsyncResultHandler) {
        Future<Void> startFuture = Future.future();
        startFuture.setHandler(result -> {
            if (result.failed()) {
                vertx.runOnContext(v->messageStreamAsyncResultHandler.handle(Future.failedFuture(result.cause())));

                return;
            }

            final FilterStream filterStream = new FilterStream(messageInjector);
            vertx.runOnContext(v->messageStreamAsyncResultHandler.handle(Future.succeededFuture(new MessageStream(filterStream, filterStream))));

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
        timeout = configObj.getInteger("timeout", DEFAULT_TIMEOUT);
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
        private ConcurrentLinkedQueue<PipelinePack> readBuffers = new ConcurrentLinkedQueue<>();

        private Handler<PipelinePack> dataHandler;
        private boolean paused;
        private Handler<Void> drainHandler;
        private Handler<Throwable> exceptionHandler;
        private Future<Void> writeCompleteFuture;
        private Handler<Void> endHandler;

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
        public AsyncWriteStream<PipelinePack> write(PipelinePack data, Future<Void> future, PipelinePack context) {

            try {
                if (timeout > 0) {
                    vertx.setTimer(timeout, id -> {
                        if (future.isComplete()) return;

                        future.fail("Filter runtime exceeded timeout.");
                    });
                }
                handleRequest(data, future, pack -> {
                    readBuffers.add(pack);

                    if (paused) {
                        return;
                    }

                    purgeReadBuffers();
                }, messageInjector);
            } catch (Throwable throwable) {
                future.fail(throwable);
            }
            return this;
        }

        protected void purgeReadBuffers() {
            while (!readBuffers.isEmpty() && !paused) {
                final PipelinePack nextPack = readBuffers.poll();
                if (nextPack != null) {
                    if (dataHandler != null) dataHandler.handle(nextPack);
                }
            }
        }

        @Override
        public WriteStream<PipelinePack> write(PipelinePack pipelinePack) {
            Objects.requireNonNull(writeCompleteFuture, "writeCompleteFuture required. This should be set automatically by the MuxRegistration.");
            Objects.requireNonNull(dataHandler, "dataHandler required. This should be set automatically by the MuxRegistration.");
            return write(pipelinePack, writeCompleteFuture, pipelinePack);
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
            vertx.runOnContext(v -> callDrainHandler());
            return this;
        }

        @Override
        public ReadStream<PipelinePack> handler(Handler<PipelinePack> dataHandler) {
            this.dataHandler = dataHandler;
            if (dataHandler != null) purgeReadBuffers();
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
            purgeReadBuffers();
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
            this.endHandler = endHandler;
            return this;
        }

        @Override
        public void stop(WriteStreamPool pool) {
            //no op
        }

        @Override
        public OutPipeline writeCompleteFuture(Future<Void> future) {
            writeCompleteFuture = future;
            return this;
        }
    }

    /**
     * Concrete instances implement this method which provides asynchronous request/reply semantics as well as a
     * messageInjector which can be used to send new messages to the mux.
     * @param pipelinePack Incoming pipeline pack.
     * @param writeCompleteFuture Future to indicate that the filter is completely done. Can be called asynchronously.
     * @param dataHandler Handler to return data bidirectionally through the Mux to the Input.
     * @param messageInjector A proxy for sending new messages into the Mux. Messages must not match the current filters
     *                        messageMatcher. An error is thrown if this happens to prevent infinite loops.
     */
    public abstract void handleRequest(PipelinePack pipelinePack, Future<Void> writeCompleteFuture, Handler<PipelinePack> dataHandler, MessageInjector messageInjector);
}