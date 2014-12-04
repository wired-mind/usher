package io.cozmic.usherprotocols.core;

import io.cozmic.pulsar.core.parsing.CozmicParser;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by chuck on 9/30/14.
 */
public class CozmicSocket implements ReadStream<CozmicSocket>, WriteStream<CozmicSocket>, MessageReadStream<CozmicSocket> {
    public static final int LENGTH_HEADER_SIZE = 4;
    private final NetSocket sock;
    private ConcurrentLinkedQueue<Message> readBuffers = new ConcurrentLinkedQueue<>();

    CozmicParser cozmicParser = new CozmicParser();

    private Handler<Buffer> dataHandler;
    private boolean paused;
    private Handler<Message> messageHandler;

    public CozmicSocket(NetSocket sock) {
        this.sock = sock;

        cozmicParser.handler(new Handler<Buffer>() {
            @Override
            public void handle(Buffer buff) {

                int pos = 0;
                final int messageIdLength = buff.getInt(pos);
                pos += 4;
                String messageId = buff.getString(pos, pos + messageIdLength);

                pos += messageIdLength;
                final Buffer body = buff.getBuffer(pos, buff.length());

                readBuffers.add(new Message(messageId, body));

                if (paused) {
                    return;
                }

                purgeReadBuffers();
            }
        });

        sock.dataHandler(cozmicParser);
    }


    protected void purgeReadBuffers() {
        while (!readBuffers.isEmpty() && !paused) {
            final Message nextMessage = readBuffers.poll();
            if (nextMessage != null) {
                if (dataHandler != null) dataHandler.handle(nextMessage.getBody());
                if (messageHandler != null) messageHandler.handle(nextMessage);
            }
        }
    }

    @Override
    public CozmicSocket setWriteQueueMaxSize(int maxSize) {
        sock.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return sock.writeQueueFull();
    }

    @Override
    public CozmicSocket drainHandler(Handler<Void> handler) {
        sock.drainHandler(handler);
        return this;
    }

    @Override
    public CozmicSocket endHandler(Handler<Void> endHandler) {
        sock.endHandler(endHandler);
        return this;
    }

    @Override
    public CozmicSocket dataHandler(final Handler<Buffer> handler) {
        this.dataHandler = handler;
        return this;
    }

    @Override
    public CozmicSocket messageHandler(Handler<Message> messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    @Override
    public CozmicSocket pause() {
        paused = true;
        sock.pause();
        return this;
    }

    @Override
    public CozmicSocket resume() {
        paused = false;
        purgeReadBuffers();
        sock.resume();
        return this;
    }

    @Override
    public CozmicSocket write(Buffer data) {
        int messageLength = 4 + data.length();
        final Buffer envelope = new Buffer(messageLength);
        envelope.appendInt(messageLength);
        envelope.appendBuffer(data);

        sock.write(envelope);
        return this;
    }

    @Override
    public CozmicSocket exceptionHandler(Handler<Throwable> handler) {
        sock.exceptionHandler(handler);
        return this;
    }

    public MessageReadStream<?> translate(CozmicStreamProcessor cozmicStreamProcessor) {
        return new CozmicStreamProcessingReadStream(this, cozmicStreamProcessor);
    }

}
