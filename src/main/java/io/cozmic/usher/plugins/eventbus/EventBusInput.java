package io.cozmic.usher.plugins.eventbus;

import io.cozmic.usher.core.InputPlugin;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.cozmic.usher.streams.AsyncWriteStream;
import io.cozmic.usher.streams.ClosableWriteStream;
import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 10/7/15.
 * <p>
 *     This started out as a fake. Making a "legit" eventbus input. Might not be fully baked for non-test uses just yet.
 */
public class EventBusInput implements InputPlugin {
    private JsonObject configObj;
    private Vertx vertx;
    private MessageConsumer<Buffer> inputConsumer;
    private MessageProducer<Buffer> outputPublisher;
    private MessageProducer<Buffer> completePublisher;
    private MessageProducer<Buffer> closePublisher;

    @Override
    public void init(JsonObject configObj, Vertx vertx) throws UsherInitializationFailedException {

        this.configObj = configObj;
        this.vertx = vertx;

    }

    @Override
    public void stop(AsyncResultHandler<Void> stopHandler) {
        inputConsumer.unregister(stopHandler);
    }

    @Override
    public void run(AsyncResultHandler<Void> startupHandler, Handler<DuplexStream<Buffer, Buffer>> duplexStreamHandler) {
        final EventBus eventBus = vertx.eventBus();
        inputConsumer = eventBus.consumer(configObj.getString("input.address"));
        outputPublisher = eventBus.publisher(configObj.getString("output.address"));
        completePublisher = eventBus.publisher(configObj.getString("complete.address"));
        closePublisher = eventBus.publisher(configObj.getString("close.address"));

        final EventBusClosableWriteStream eventBusStream = new EventBusClosableWriteStream(inputConsumer, outputPublisher);
        final DuplexStream<Buffer, Buffer> duplexStream = new DuplexStream<>(inputConsumer.bodyStream(), eventBusStream);
        duplexStream
                .closeHandler(v -> {
                    closePublisher.write(Buffer.buffer("Closed"));
                })
                .writeCompleteHandler(pack -> {
                    completePublisher.write(Buffer.buffer("Ok"));
                });
        vertx.runOnContext(v -> {
            duplexStreamHandler.handle(duplexStream);
        });
        startupHandler.handle(Future.succeededFuture());
    }

    /**
     *
     */
    private static class EventBusClosableWriteStream implements ClosableWriteStream<Buffer> {
        private final MessageConsumer<Buffer> inputConsumer;
        private final MessageProducer<Buffer> outputPublisher;

        public EventBusClosableWriteStream(MessageConsumer<Buffer> inputConsumer, MessageProducer<Buffer> outputPublisher) {
            this.inputConsumer = inputConsumer;
            this.outputPublisher = outputPublisher;
        }

        @Override
        public void close() {
            //stop the consumer
            inputConsumer.unregister();
        }

        @Override
        public AsyncWriteStream<Buffer> write(Buffer data, Handler<AsyncResult<Void>> writeCompleteHandler) {
            outputPublisher.write(data);
            if (writeCompleteHandler != null) writeCompleteHandler.handle(Future.succeededFuture());
            return this;
        }

        @Override
        public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
            outputPublisher.exceptionHandler(handler);
            return this;
        }

        @Override
        public WriteStream<Buffer> write(Buffer data) {
            outputPublisher.write(data);
            return this;
        }

        @Override
        public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
            outputPublisher.setWriteQueueMaxSize(maxSize);
            return this;
        }

        @Override
        public boolean writeQueueFull() {
            return outputPublisher.writeQueueFull();
        }

        @Override
        public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
            outputPublisher.drainHandler(handler);
            return this;
        }
    }
}
