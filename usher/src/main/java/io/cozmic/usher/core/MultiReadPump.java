package io.cozmic.usher.core;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.ConcurrentHashSet;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;

/**
 * Created by chuck on 10/2/14.
 */
public class MultiReadPump {

    private final ConcurrentHashSet<ReadStream<?>> readStreams = new ConcurrentHashSet<>();
    private final WriteStream<?> writeStream;
    private int pumped;


    public static MultiReadPump createPump(WriteStream<?> ws) {
        return new MultiReadPump(ws);
    }


    public static MultiReadPump createPump(WriteStream<?> ws, int writeQueueMaxSize) {
        return new MultiReadPump(ws, writeQueueMaxSize);
    }

    /**
     * Set the write queue max size to {@code maxSize}
     */
    public MultiReadPump setWriteQueueMaxSize(int maxSize) {
        this.writeStream.setWriteQueueMaxSize(maxSize);
        return this;
    }

    /**
     * Start the Pump. The Pump can be started and stopped multiple times.
     */
    public MultiReadPump add(ReadStream<?> rs) {
        rs.dataHandler(dataHandler);
        readStreams.add(rs);
        return this;
    }

    /**
     * Stop the Pump. The Pump can be started and stopped multiple times.
     */
    public MultiReadPump remove(ReadStream<?> rs) {
        if (readStreams.remove(rs)) {
            rs.dataHandler(null);
        }
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
            for (ReadStream<?> readStream : readStreams) {
                readStream.resume();
            }
        }
    };

    private final Handler<Buffer> dataHandler = new Handler<Buffer>() {
        public void handle(Buffer buffer) {
            writeStream.write(buffer);
            pumped += buffer.length();
            if (writeStream.writeQueueFull()) {
                for (ReadStream<?> readStream : readStreams) {
                    readStream.pause();
                }

                writeStream.drainHandler(drainHandler);
            }
        }
    };

    /**
     * Create a new {@code Pump} with the given {@code ReadStream} and {@code WriteStream}. Set the write queue max size
     * of the write stream to {@code maxWriteQueueSize}
     */
    private MultiReadPump(WriteStream<?> ws, int maxWriteQueueSize) {
        this(ws);
        this.writeStream.setWriteQueueMaxSize(maxWriteQueueSize);
    }

    private MultiReadPump(WriteStream<?> ws) {
        this.writeStream = ws;
    }

}
