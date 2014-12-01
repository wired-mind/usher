package io.cozmic.usherprotocols.protocols;

import io.cozmic.pulsar.core.parsing.RuleBasedPacketParser;
import io.cozmic.usherprotocols.core.StreamProcessingReadStream;
import io.cozmic.usherprotocols.core.StreamProcessor;
import io.cozmic.usherprotocols.core.TranslatingReadStream;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.streams.ReadStream;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by chuck on 10/1/14.
 */
public class RuleBasedPacketSocket implements TranslatingReadStream<RuleBasedPacketSocket> {
    private final NetSocket sock;
    private final RuleBasedPacketParser ruleBasedPacketParser;
    private ConcurrentLinkedQueue<Buffer> readBuffers = new ConcurrentLinkedQueue<>();



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

    public RuleBasedPacketSocket(NetSocket sock, JsonObject responseParsingConfig) {
        this.sock = sock;
        ruleBasedPacketParser = RuleBasedPacketParser.fromConfig(responseParsingConfig, new Handler<Buffer>() {
            @Override
            public void handle(Buffer messageData) {
                readBuffers.add(messageData);

                if (paused) {
                    return;
                }

                purgeReadBuffers();
            }
        });
    }

    @Override
    public RuleBasedPacketSocket endHandler(Handler<Void> endHandler) {
        sock.endHandler(endHandler);
        return this;
    }

    @Override
    public RuleBasedPacketSocket dataHandler(final Handler<Buffer> handler) {
        this.handler = handler;
        if (handler == null) {
            sock.dataHandler(null);
        } else {

            sock.dataHandler(new Handler<Buffer>() {
                @Override
                public void handle(Buffer event) {
                    ruleBasedPacketParser.handle(event);
                }
            });
        }
        return this;
    }

    @Override
    public RuleBasedPacketSocket pause() {
        paused = true;
        sock.pause();
        return this;
    }

    @Override
    public RuleBasedPacketSocket resume() {
        paused = false;
        purgeReadBuffers();
        sock.resume();
        return this;
    }

    @Override
    public RuleBasedPacketSocket exceptionHandler(Handler<Throwable> handler) {
        sock.exceptionHandler(handler);
        return this;
    }
    public ReadStream<?> translate(StreamProcessor streamProcessor) {
        return new StreamProcessingReadStream(this, streamProcessor);
    }
}