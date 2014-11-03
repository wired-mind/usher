package io.cozmic.usherprotocols.protocols;

import io.cozmic.usherprotocols.core.StreamProcessingReadStream;
import io.cozmic.usherprotocols.core.StreamProcessor;
import io.cozmic.usherprotocols.core.TranslatingReadStream;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;
import org.vertx.java.core.streams.ReadStream;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by chuck on 10/27/14.
 */
public class FixedLengthSocket implements TranslatingReadStream<FixedLengthSocket> {
    private final NetSocket sock;

    private ConcurrentLinkedQueue<Buffer> readBuffers = new ConcurrentLinkedQueue<>();


    private int size;


    protected void purgeReadBuffers() {

        while (!readBuffers.isEmpty() && !paused) {
            final Buffer nextBuff = readBuffers.poll();
            if (nextBuff != null) {
                handler.handle(nextBuff);
            }
        }
    }

    private Handler<Buffer> handler;
    private boolean paused;

    public FixedLengthSocket(NetSocket sock, int size) {
        this.sock = sock;
        this.size = size;
    }

    @Override
    public FixedLengthSocket endHandler(Handler<Void> endHandler) {
        sock.endHandler(endHandler);
        return this;
    }

    @Override
    public FixedLengthSocket dataHandler(final Handler<Buffer> handler) {
        final RecordParser recordParser = RecordParser.newFixed(this.size, new Handler<Buffer>() {
            @Override
            public void handle(Buffer reqBuffer) {
                readBuffers.add(reqBuffer);

                if (paused) {
                    return;
                }

                purgeReadBuffers();
            }
        });
        this.handler = handler;
        if (handler == null) {
            sock.dataHandler(null);
        } else {

            sock.dataHandler(new Handler<Buffer>() {
                @Override
                public void handle(Buffer event) {
                    recordParser.handle(event);
                }
            });
        }
        return this;
    }

    @Override
    public FixedLengthSocket pause() {
        paused = true;
        sock.pause();
        return this;
    }

    @Override
    public FixedLengthSocket resume() {
        paused = false;
        purgeReadBuffers();
        sock.resume();
        return this;
    }

    @Override
    public FixedLengthSocket exceptionHandler(Handler<Throwable> handler) {
        sock.exceptionHandler(handler);
        return this;
    }
    public ReadStream<?> translate(StreamProcessor streamProcessor) {
        return new StreamProcessingReadStream(this, streamProcessor);
    }}
