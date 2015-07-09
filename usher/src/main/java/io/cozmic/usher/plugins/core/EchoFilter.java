package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.FilterPlugin;
import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * Simplest example filter that does an Echo. Not implementing flow control.
 */
public class EchoFilter implements FilterPlugin {


    @Override
    public void init(JsonObject configObj, Vertx vertx) {

    }

    @Override
    public void run(AsyncResultHandler<DuplexStream<Buffer, Buffer>> duplexStreamAsyncResultHandler) {
        final EchoStream echoStream = new EchoStream();
        duplexStreamAsyncResultHandler.handle(Future.succeededFuture(new DuplexStream<>(echoStream, echoStream)));
    }


    private class EchoStream implements ReadStream<Buffer>, WriteStream<Buffer> {
        private Handler<Buffer> dataHandler;
        @Override
        public EchoStream exceptionHandler(Handler<Throwable> handler) {
            return this;
        }

        @Override
        public WriteStream<Buffer> write(Buffer data) {
            dataHandler.handle(data);
            return this;
        }

        @Override
        public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
            return this;
        }

        @Override
        public boolean writeQueueFull() {
            return false;
        }

        @Override
        public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
            return this;
        }

        @Override
        public ReadStream<Buffer> handler(Handler<Buffer> dataHandler) {
            this.dataHandler = dataHandler;
            return this;
        }

        @Override
        public ReadStream<Buffer> pause() {
            return this;
        }

        @Override
        public ReadStream<Buffer> resume() {
            return this;
        }

        @Override
        public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
            return this;
        }

    }
}
