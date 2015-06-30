package io.cozmic.usherprotocols.protocols;

import io.cozmic.usherprotocols.core.StreamProcessingReadStream;
import io.cozmic.usherprotocols.core.StreamProcessor;
import io.cozmic.usherprotocols.core.TranslatingReadStream;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;



import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by chuck on 10/22/14.
 *
 * Just adding a quick example of what this might look like. Will need testing and more work
 */
public class HttpSocket implements TranslatingReadStream<Buffer> {
    private final NetSocket sock;
    private Handler<Buffer> handler;
    private boolean paused;
    private ConcurrentLinkedQueue<Buffer> readBuffers = new ConcurrentLinkedQueue<>();

    public static final String HTTP_DELIM = "\r\n\r\n";

    private final RecordParser rawHttpParser = RecordParser.newDelimited(HTTP_DELIM, new Handler<Buffer>() {
        @Override
        public void handle(Buffer reqBuffer) {
            readBuffers.add(reqBuffer.appendString(HTTP_DELIM));

            if (paused) {
                return;
            }

            purgeReadBuffers();
        }
    });

    public HttpSocket(NetSocket sock) {
        this.sock = sock;
    }

    protected void purgeReadBuffers() {

        while (!readBuffers.isEmpty() && !paused) {
            final Buffer nextBuff = readBuffers.poll();
            if (nextBuff != null) {
                handler.handle(nextBuff);
            }
        }
    }


    @Override
    public HttpSocket endHandler(Handler<Void> endHandler) {
        sock.endHandler(endHandler);
        return this;
    }

    @Override
    public HttpSocket handler(final Handler<Buffer> handler) {
        this.handler = handler;
        if (handler == null) {
            sock.handler(null);
        } else {

            sock.handler(new Handler<Buffer>() {
                @Override
                public void handle(Buffer event) {
                    rawHttpParser.handle(event);
                }
            });
        }
        return this;
    }

    @Override
    public HttpSocket pause() {
        paused = true;
        sock.pause();
        return this;
    }

    @Override
    public HttpSocket resume() {
        paused = false;
        purgeReadBuffers();
        sock.resume();
        return this;
    }

    @Override
    public HttpSocket exceptionHandler(Handler<Throwable> handler) {
        sock.exceptionHandler(handler);
        return this;
    }
    @Override
    public ReadStream<Buffer> translate(StreamProcessor streamProcessor) {
        return new StreamProcessingReadStream(this, streamProcessor);
    }
}
