package io.cozmic.usher.journal;


import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.streams.WriteStream;

/**
 * Created by chuck on 9/15/14.
 */
public class JournalingWriteStream implements WriteStream<JournalingWriteStream> {

    private final NetSocket socket;
    private final JournalEventProducer producer;

    public JournalingWriteStream(NetSocket socket, JournalEventProducer producer) {
        this.socket = socket;
        this.producer = producer;
    }


    @Override
    public JournalingWriteStream write(Buffer data) {
        producer.onData(data, socket);
        //return socket;
        socket.write(data);
        return this;
    }

    @Override
    public JournalingWriteStream setWriteQueueMaxSize(int maxSize) {
        socket.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return socket.writeQueueFull();
    }

    @Override
    public JournalingWriteStream drainHandler(Handler<Void> handler) {
        socket.drainHandler(handler);
        return this;
    }

    @Override
    public JournalingWriteStream exceptionHandler(Handler<Throwable> handler) {
        socket.exceptionHandler(handler);
        return this;
    }
}
