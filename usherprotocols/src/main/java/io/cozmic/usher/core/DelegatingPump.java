package io.cozmic.usher.core;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;

/**
 * Created by chuck on 9/30/14.
 */
public class DelegatingPump {
    private final ReadStream<?> readStream;
    private final WriteStream<?> writeStream;
    private final StreamProcessor streamProcessor;
    private int pumped;

    /**
     * Create a new {@code Pump} with the given {@code ReadStream} and {@code WriteStream}
     */
    public static DelegatingPump createPump(ReadStream<?> rs, WriteStream<?> ws, StreamProcessor streamProcessor1) {
        return new DelegatingPump(rs, ws, streamProcessor1);
    }

    /**
     * Create a new {@code Pump} with the given {@code ReadStream} and {@code WriteStream} and
     * {@code writeQueueMaxSize}
     */
    public static DelegatingPump createPump(ReadStream<?> rs, WriteStream<?> ws, int writeQueueMaxSize, StreamProcessor streamProcessor1) {
        return new DelegatingPump(rs, ws, writeQueueMaxSize, streamProcessor1);
    }

    /**
     * Set the write queue max size to {@code maxSize}
     */
    public DelegatingPump setWriteQueueMaxSize(int maxSize) {
        this.writeStream.setWriteQueueMaxSize(maxSize);
        return this;
    }

    /**
     * Start the Pump. The Pump can be started and stopped multiple times.
     */
    public DelegatingPump start() {
        readStream.dataHandler(dataHandler);
        return this;
    }

    /**
     * Stop the Pump. The Pump can be started and stopped multiple times.
     */
    public DelegatingPump stop() {
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

    private final Handler<Buffer> dataHandler = new Handler<Buffer>() {
        public void handle(Buffer buffer) {
            streamProcessor.process(buffer, new Handler<AsyncResult<Buffer>>() {
                @Override
                public void handle(AsyncResult<Buffer> event) {
                    if (event.failed()) {
                        //TODO: real logging
                        System.out.println(event.cause().getMessage());
                        return;
                    }

                    final Buffer result = event.result();
                    writeStream.write(result);
                    pumped += result.length();
                    if (writeStream.writeQueueFull()) {
                        readStream.pause();
                        writeStream.drainHandler(drainHandler);
                    }
                }
            });

        }
    };

    /**
     * Create a new {@code Pump} with the given {@code ReadStream} and {@code WriteStream}. Set the write queue max size
     * of the write stream to {@code maxWriteQueueSize}
     */
    private DelegatingPump(ReadStream<?> rs, WriteStream<?> ws, int maxWriteQueueSize, StreamProcessor streamProcessor) {
        this(rs, ws, streamProcessor);
        this.writeStream.setWriteQueueMaxSize(maxWriteQueueSize);
    }

    private DelegatingPump(ReadStream<?> rs, WriteStream<?> ws, StreamProcessor streamProcessor) {
        this.readStream = rs;
        this.writeStream = ws;
        this.streamProcessor = streamProcessor;
    }


}
