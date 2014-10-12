package io.cozmic.usher.core;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by chuck on 10/1/14.
 */
public class RoutingWriteStream implements WriteStream<RoutingWriteStream> {
    private ConcurrentHashMap<String, NetSocket> inflightMessages = new ConcurrentHashMap<>();
    @Override
    public RoutingWriteStream write(Buffer buffer) {
        int pos = 0;
        final int messageIdLength = buffer.getInt(pos);
        pos += 4;
        String messageId = buffer.getString(pos, pos + messageIdLength);
        pos += messageIdLength;
        final NetSocket netSocket = inflightMessages.get(messageId);
        final Buffer body = buffer.getBuffer(pos, buffer.length());
        netSocket.write(body);
        return this;
    }

    @Override
    public RoutingWriteStream setWriteQueueMaxSize(int maxSize) {
        return null;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public RoutingWriteStream drainHandler(Handler<Void> handler) {
        return null;
    }

    @Override
    public RoutingWriteStream exceptionHandler(Handler<Throwable> handler) {
        return null;
    }

    public void addMessage(String messageId, NetSocket socket) {
        inflightMessages.put(messageId, socket);
    }
}
