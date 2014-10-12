package io.cozmic.usher.core;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.streams.WriteStream;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by chuck on 10/2/14.
 */
public class CozmicPump {

    private final MessageReadStream<?> messageReadStream;
    private final ConcurrentHashMap<String, WriteStream<?>> writeStreams = new ConcurrentHashMap<>();
    private int pumped;

    /**
     * Create a new {@code Pump} with the given {@code ReadStream} and {@code WriteStream}
     */
    public static CozmicPump createPump(MessageReadStream<?> messageReadStream) {
        return new CozmicPump(messageReadStream);
    }




    /**
     * Start the Pump. The Pump can be started and stopped multiple times.
     */
    public CozmicPump start() {
        messageReadStream.messageHandler(messageHandler);
        return this;
    }

    public CozmicPump add(String messageId, WriteStream<?> writeStream) {
        writeStreams.put(messageId, writeStream);
        return this;
    }

    /**
     * Stop the Pump. The Pump can be started and stopped multiple times.
     */
    public CozmicPump stop() {
        for (WriteStream<?> writeStream : writeStreams.values()) {
            writeStream.drainHandler(null);
        }

        messageReadStream.messageHandler(null);
        return this;
    }

    /**
     * Return the total number of bytes pumped by this pump.
     */
    public int bytesPumped() {
        return pumped;
    }

    private final Handler<Void> drainHandler = new Handler<Void>() {
        public void handle(Void v) {
            messageReadStream.resume();
        }
    };

    private final Handler<Message> messageHandler = new Handler<Message>() {
        public void handle(Message message) {

            final WriteStream<?> writeStream = writeStreams.remove(message.getMessageId());
            if (writeStream != null) {

                final Buffer buffer = message.getData();
                writeStream.write(buffer);
                pumped += buffer.length();
                if (writeStream.writeQueueFull()) {
                    messageReadStream.pause();
                    writeStream.drainHandler(drainHandler);
                }
            }
        }
    };



    private CozmicPump(MessageReadStream<?> messageReadStream) {
        this.messageReadStream = messageReadStream;
    }


}
