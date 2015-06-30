package io.cozmic.usherprotocols.core;



import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by chuck on 10/2/14.
 */
public class CozmicPump {

    private MessageReadStream messageReadStream;
    private final ConcurrentHashMap<String, WriteStream<Buffer>> writeStreams = new ConcurrentHashMap<>();
    private int pumped;
    private Handler<Request> inflightHandler;
    private Handler<String> responseHandler;

    /**
     * Create a new {@code Pump}
     */
    public static CozmicPump createPump() {
        return new CozmicPump();
    }





    public CozmicPump setMessageReadStream(MessageReadStream messageReadStream) {
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

    public CozmicPump add(Request request, WriteStream<Buffer> writeStream) {
        if (inflightHandler != null) {
            inflightHandler.handle(request);
        }
        writeStreams.put(request.getMessageId(), writeStream);
        return this;
    }


    public void timeoutMessage(String messageId) {
        timeoutMessage(messageId, null);
    }

    public void timeoutMessage(String messageId, Buffer buffer) {
        final WriteStream<Buffer> writeStream = writeStreams.remove(messageId);
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
            final WriteStream<Buffer> writeStream = writeStreams.remove(message.getMessageId());
            if (writeStream != null) {
                final Buffer buffer = message.getBody();
                doWrite(writeStream, buffer);
                if (responseHandler != null) {
                    responseHandler.handle(message.getMessageId());
                }
            }
        }
    };

    protected void doWrite(WriteStream<Buffer> writeStream, Buffer buffer) {
        writeStream.write(buffer);
        pumped += buffer.length();
        if (writeStream.writeQueueFull()) {
            messageReadStream.pause();
            writeStream.drainHandler(drainHandler);
        }
    }


    private CozmicPump() {

    }

    public void inflightHandler(Handler<Request> inflightHandler) {

        this.inflightHandler = inflightHandler;
    }

    public void responseHandler(Handler<String> responseHandler) {

        this.responseHandler = responseHandler;
    }
}
