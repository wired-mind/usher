package io.cozmic.usherprotocols.core;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.streams.WriteStream;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by chuck on 10/2/14.
 */
public class CozmicPump {

    private MessageReadStream<?> messageReadStream;
    private final ConcurrentHashMap<String, WriteStream<?>> writeStreams = new ConcurrentHashMap<>();
    private int pumped;
    private Handler<Message> inflightHandler;
    private Handler<String> responseHandler;

    /**
     * Create a new {@code Pump}
     */
    public static CozmicPump createPump() {
        return new CozmicPump();
    }





    public CozmicPump setMessageReadStream(MessageReadStream<?> messageReadStream) {
        if (this.messageReadStream != null) {
            this.messageReadStream.messageHandler(null);
        }

        this.messageReadStream = messageReadStream;
        if (messageReadStream != null) {
            messageReadStream.messageHandler(messageHandler);
        } else {
            for (WriteStream<?> writeStream : writeStreams.values()) {
                writeStream.drainHandler(null);
            }
        }

        return this;
    }

    public CozmicPump add(Message message, WriteStream<?> writeStream) {
        if (inflightHandler != null) {
            inflightHandler.handle(message);
        }
        writeStreams.put(message.getMessageId(), writeStream);
        return this;
    }


    public void timeoutMessage(String messageId) {
        timeoutMessage(messageId, null);
    }

    public void timeoutMessage(String messageId, Buffer buffer) {
        final WriteStream<?> writeStream = writeStreams.remove(messageId);
        if (writeStream != null) {

            if (buffer != null) {
                doWrite(writeStream, buffer);
            }
            if (responseHandler != null) {
                responseHandler.handle(messageId);
            }
        }
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
                final Buffer buffer = message.getBody();
                doWrite(writeStream, buffer);
                if (responseHandler != null) {
                    responseHandler.handle(message.getMessageId());
                }
            }
        }
    };

    protected void doWrite(WriteStream<?> writeStream, Buffer buffer) {
        writeStream.write(buffer);
        pumped += buffer.length();
        if (writeStream.writeQueueFull()) {
            messageReadStream.pause();
            writeStream.drainHandler(drainHandler);
        }
    }


    private CozmicPump() {

    }

    public void inflightHandler(Handler<Message> inflightHandler) {

        this.inflightHandler = inflightHandler;
    }

    public void responseHandler(Handler<String> responseHandler) {

        this.responseHandler = responseHandler;
    }
}
