package io.cozmic.usherprotocols.protocols;

import io.cozmic.usherprotocols.core.StreamProcessingReadStream;
import io.cozmic.usherprotocols.core.StreamProcessor;
import io.cozmic.usherprotocols.core.TranslatingReadStream;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;
import org.vertx.java.core.streams.ReadStream;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by chuck on 10/1/14.
 */
public class ConfigurablePacketSocket implements TranslatingReadStream<ConfigurablePacketSocket> {
    private final NetSocket sock;


    private final int packetTypeLength = 1;
    private final byte packetResetByte = 0x00;
    private ConcurrentLinkedQueue<Buffer> readBuffers = new ConcurrentLinkedQueue<>();

    Map<Byte, Integer> config = new HashMap<>();

    protected final RecordParser rawPacketParser = RecordParser.newFixed(packetTypeLength, new Handler<Buffer>() {
        byte type = packetResetByte;
        Buffer typeBuffer;
        @Override
        public void handle(Buffer reqBuffer) {
            boolean newPacket = type == packetResetByte;
            if (newPacket) {
                typeBuffer = reqBuffer;
                type = reqBuffer.getByte(0);

                final Integer size = config.get(type);
                final int remainingPacketLength = size - packetTypeLength;
                rawPacketParser.fixedSizeMode(remainingPacketLength);

                return;
            }

            final Buffer messageData = typeBuffer.appendBuffer(reqBuffer);

            //reset
            type = packetResetByte;
            rawPacketParser.fixedSizeMode(packetTypeLength);


            readBuffers.add(messageData);

            if (paused) {
                return;
            }

            purgeReadBuffers();
        }
    });



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

    public ConfigurablePacketSocket(NetSocket sock, JsonArray packetMap) {
        this.sock = sock;
        if (!packetMap.isArray()) {
            throw new RuntimeException("Expecting an array of strings as type:length pairs");
        }
        final List pairs = packetMap.toList();
        for (Object pair : pairs) {
            final String[] pairEntry = ((String) pair).split(":");
            config.put(Byte.parseByte(pairEntry[0]), Integer.parseInt(pairEntry[1]));
        }
    }

    @Override
    public ConfigurablePacketSocket endHandler(Handler<Void> endHandler) {
        sock.endHandler(endHandler);
        return this;
    }

    @Override
    public ConfigurablePacketSocket dataHandler(final Handler<Buffer> handler) {
        this.handler = handler;
        if (handler == null) {
            sock.dataHandler(null);
        } else {

            sock.dataHandler(new Handler<Buffer>() {
                @Override
                public void handle(Buffer event) {
                    rawPacketParser.handle(event);
                }
            });
        }
        return this;
    }

    @Override
    public ConfigurablePacketSocket pause() {
        paused = true;
        sock.pause();
        return this;
    }

    @Override
    public ConfigurablePacketSocket resume() {
        paused = false;
        purgeReadBuffers();
        sock.resume();
        return this;
    }

    @Override
    public ConfigurablePacketSocket exceptionHandler(Handler<Throwable> handler) {
        sock.exceptionHandler(handler);
        return this;
    }
    public ReadStream<?> translate(StreamProcessor streamProcessor) {
        return new StreamProcessingReadStream(this, streamProcessor);
    }
}
