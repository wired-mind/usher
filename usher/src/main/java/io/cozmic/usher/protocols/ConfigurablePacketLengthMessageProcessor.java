package io.cozmic.usher.protocols;

import io.cozmic.usher.core.Message;
import io.cozmic.usher.core.MessageProcessor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.parsetools.RecordParser;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by chuck on 9/15/14.
 */
public class ConfigurablePacketLengthMessageProcessor implements MessageProcessor {
    private Handler<Message> output;
    private final int packetTypeLength = 1;
    private final byte packetResetByte = 0x00;

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

            getOutput().handle(new Message(messageData));

            //reset
            type = packetResetByte;
            rawPacketParser.fixedSizeMode(packetTypeLength);

        }
    });



    @Override
    public void setOutput(Handler<Message> output) {
        this.output = output;
    }

    private Handler<Message> getOutput() {
        return output;
    }

    public void handle(final Message message) {
        rawPacketParser.handle(message.getData());
    }



}
