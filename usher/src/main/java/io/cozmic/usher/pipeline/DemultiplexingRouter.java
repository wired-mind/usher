package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.OutputRunner;
import io.cozmic.usher.core.Router;
import io.cozmic.usher.message.Message;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import java.util.List;

/**
 * Created by chuck on 7/3/15.
 */
public class DemultiplexingRouter implements Router {
    public DemultiplexingRouter(List<OutputRunner> outputRunners) {
    }

    @Override
    public Router exceptionHandler(Handler<Throwable> handler) {
        return null;
    }

    @Override
    public WriteStream<Message> write(Message data) {
        return null;
    }

    @Override
    public WriteStream<Message> setWriteQueueMaxSize(int maxSize) {
        return null;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public WriteStream<Message> drainHandler(Handler<Void> handler) {
        return null;
    }

    @Override
    public ReadStream<Message> handler(Handler<Message> handler) {
        return null;
    }

    @Override
    public ReadStream<Message> pause() {
        return null;
    }

    @Override
    public ReadStream<Message> resume() {
        return null;
    }

    @Override
    public ReadStream<Message> endHandler(Handler<Void> endHandler) {
        return null;
    }
}
