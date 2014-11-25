package io.cozmic.usher.peristence;


import io.cozmic.usherprotocols.core.Request;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.streams.WriteStream;

/**
 * Created by chuck on 9/15/14.
 */
public class JournalingWriteStream implements WriteStream<JournalingWriteStream> {

    private final WriteStream<?> writeStream;
    private final RequestEventProducer producer;

    public JournalingWriteStream(WriteStream<?> writeStream, RequestEventProducer producer) {
        this.writeStream = writeStream;
        this.producer = producer;
    }


    @Override
    public JournalingWriteStream write(Buffer data) {
        producer.onData(Request.fromEnvelope(data));
        writeStream.write(data);
        return this;
    }

    @Override
    public JournalingWriteStream setWriteQueueMaxSize(int maxSize) {
        writeStream.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return writeStream.writeQueueFull();
    }

    @Override
    public JournalingWriteStream drainHandler(Handler<Void> handler) {
        writeStream.drainHandler(handler);
        return this;
    }

    @Override
    public JournalingWriteStream exceptionHandler(Handler<Throwable> handler) {
        writeStream.exceptionHandler(handler);
        return this;
    }
}
