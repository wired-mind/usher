package io.cozmic.usher.protocols;

import io.cozmic.usher.core.Counter;
import io.cozmic.usher.core.CozmicStreamProcessingReadStream;
import io.cozmic.usher.core.StreamProcessingReadStream;
import io.cozmic.usher.core.StreamProcessor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.ConcurrentHashSet;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;
import org.vertx.java.core.shareddata.ConcurrentSharedMap;
import org.vertx.java.core.streams.ReadStream;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by chuck on 10/1/14.
 */
public class ConfigurablePacketSocket implements ReadStream<ConfigurablePacketSocket> {
    private final NetSocket sock;
    private final Counter counter;
    private final Counter receivedBytesCounter;

    private final int packetTypeLength = 1;
    private final byte packetResetByte = 0x00;
    private ConcurrentLinkedQueue<Buffer> readBuffers = new ConcurrentLinkedQueue<>();

    Map<Byte, Integer> config = new HashMap<Byte, Integer>(){{
        put((byte) 0x03, 28);
        put((byte) 0x01, 34);
    }};

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

    public ConfigurablePacketSocket(NetSocket socket, Counter counter) {
        this(socket, counter, null);
    }


    protected void purgeReadBuffers() {

        while (!readBuffers.isEmpty() && !paused) {
            final Buffer nextBuff = readBuffers.poll();
            if (nextBuff != null) {
                final int count = counter.MyCounter.incrementAndGet();


//                if (count % 1000 == 0) {
//                    System.out.println(counter.getName() + ": " + count);
//                }

//        if (counter.getName() == "purged_proxy")
//            System.out.println(counter.getName() + ": " + count);
                handler.handle(nextBuff);
            }
        }







    }

    private Handler<Buffer> handler;
    private boolean paused;

    public ConfigurablePacketSocket(NetSocket sock, Counter counter, Counter receivedBytesCounter) {

        this.sock = sock;

        this.counter = counter;
        this.receivedBytesCounter = receivedBytesCounter;
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

                    if (receivedBytesCounter != null) {

                        final int count = receivedBytesCounter.MyCounter.addAndGet(event.length());


                    }
                    rawPacketParser.handle(event);
                }
            });
        }
        return this;
    }

    @Override
    public ConfigurablePacketSocket pause() {
       // System.out.println("ConfigurablePacketSocket Pausing");
        paused = true;
        sock.pause();
        return this;
    }

    @Override
    public ConfigurablePacketSocket resume() {
      //  System.out.println("ConfigurablePacketSocket resuming");
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
    public ReadStream<?> translate(StreamProcessor cozmicStreamProcessor) {
        return new StreamProcessingReadStream(this, cozmicStreamProcessor);
    }
}
