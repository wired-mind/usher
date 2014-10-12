
package io.cozmic.usher.core;


import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;

public class MessageProcessingPump {

    private final ReadStream<?> readStream;
    private final WriteStream<?> writeStream;
    private int pumped;

    /**
     * Create a new {@code Pump} with the given {@code ReadStream} and {@code WriteStream}
     */
    public static MessageProcessingPump createPump(ReadStream<?> rs, WriteStream<?> ws, MessageProcessor messageProcessor, MessageTranslator translator) {
        return new MessageProcessingPump(rs, ws, messageProcessor, translator);
    }

    /**
     * Create a new {@code Pump} with the given {@code ReadStream} and {@code WriteStream} and
     * {@code writeQueueMaxSize}
     */
    public static MessageProcessingPump createPump(ReadStream<?> rs, MessageProcessor messageProcessor, MessageTranslator translator, WriteStream<?> ws, int writeQueueMaxSize) {
        return new MessageProcessingPump(rs, ws, messageProcessor, translator, writeQueueMaxSize);
    }

    /**
     * Set the write queue max size to {@code maxSize}
     */
    public MessageProcessingPump setWriteQueueMaxSize(int maxSize) {
        this.writeStream.setWriteQueueMaxSize(maxSize);
        return this;
    }

    /**
     * Start the Pump. The Pump can be started and stopped multiple times.
     */
    public MessageProcessingPump start() {
        readStream.dataHandler(dataHandler);
        return this;
    }

    /**
     * Stop the Pump. The Pump can be started and stopped multiple times.
     */
    public MessageProcessingPump stop() {
        writeStream.drainHandler(null);
        readStream.dataHandler(null);
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
            readStream.resume();
        }
    };

    private final Handler<Buffer> dataHandler;

    /**
     * Create a new {@code Pump} with the given {@code ReadStream} and {@code WriteStream}. Set the write queue max size
     * of the write stream to {@code maxWriteQueueSize}
     */
    private MessageProcessingPump(ReadStream<?> rs, WriteStream<?> ws, MessageProcessor messageProcessor, MessageTranslator translator, int maxWriteQueueSize) {
        this(rs, ws, messageProcessor, translator);
        this.writeStream.setWriteQueueMaxSize(maxWriteQueueSize);
    }

    private MessageProcessingPump(ReadStream<?> rs, WriteStream<?> ws, final MessageProcessor messageProcessor, final MessageTranslator translator) {
        this.readStream = rs;
        this.writeStream = ws;

        //After parsing/processing a record, implement standard vertx flow control
        messageProcessor.setOutput(new Handler<Message>() {
            int count = 0;
            public void handle(Message message) {
                final Buffer buffer = translator.translate(message);
                if (buffer == null) {
                    System.out.println("No buffer, not sending");
                    return;
                }


                count++;
                writeStream.write(buffer);
//                System.out.println("Pumped: " + count);
                pumped += buffer.length();
                if (writeStream.writeQueueFull()) {
                    readStream.pause();
                    writeStream.drainHandler(drainHandler);
                }
            }
        });

        this.dataHandler = new Handler<Buffer>() {
            @Override
            public void handle(Buffer event) {
                messageProcessor.handle(new Message(event));
            }
        };
    }


}
