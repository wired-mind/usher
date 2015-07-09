package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.FilterPlugin;
import io.cozmic.usher.core.OutPipeline;
import io.cozmic.usher.core.InPipeline;
import io.cozmic.usher.core.WriteStreamPool;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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
    public void run(AsyncResultHandler<MessageStream> messageStreamAsyncResultHandler) {
        final EchoStream echoStream = new EchoStream();
        messageStreamAsyncResultHandler.handle(Future.succeededFuture(new MessageStream(echoStream, echoStream)));
    }


    private class EchoStream implements InPipeline, OutPipeline {
        private Handler<Message> dataHandler;
        @Override
        public EchoStream exceptionHandler(Handler<Throwable> handler) {
            return this;
        }

        @Override
        public WriteStream<Message> write(Message data) {
            dataHandler.handle(data);
            return this;
        }

        @Override
        public WriteStream<Message> setWriteQueueMaxSize(int maxSize) {
            return this;
        }

        @Override
        public boolean writeQueueFull() {
            return false;
        }

        @Override
        public WriteStream<Message> drainHandler(Handler<Void> handler) {
            return this;
        }

        @Override
        public ReadStream<Message> handler(Handler<Message> dataHandler) {
            this.dataHandler = dataHandler;
            return this;
        }

        @Override
        public ReadStream<Message> pause() {
            return this;
        }

        @Override
        public ReadStream<Message> resume() {
            return this;
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
}
