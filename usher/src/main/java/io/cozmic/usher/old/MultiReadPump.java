package io.cozmic.usher.old;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by chuck on 10/2/14.
 */
public class MultiReadPump {

    private final Meter requests = ProxyTunnel.metrics.meter("requests");
    private final Counter activeConnections = ProxyTunnel.metrics.counter(name(MultiReadPump.class, "active-connections"));

    private final ConcurrentHashSet<ReadStream<?>> readStreams = new ConcurrentHashSet<>();
    private WriteStream<Buffer> writeStream;
    private int pumped;


    public static MultiReadPump createPump() {
        return new MultiReadPump();
    }

    public int getConnectionCount() {
        return readStreams.size();
    }

    public MultiReadPump setWriteStream(WriteStream<Buffer> writeStream) {
        this.writeStream = writeStream;

        if (writeStream == null) {
            for (ReadStream<?> readStream : readStreams) {
                readStream.pause();
            }
            return this;
        }

        if (!writeStream.writeQueueFull()) {
            for (ReadStream<?> readStream : readStreams) {
                readStream.resume();
            }
        }
        else {
            writeStream.drainHandler(drainHandler);
        }
        return this;
    }
    public MultiReadPump setWriteStream(WriteStream<Buffer> writeStream, int maxSize) {
        this.setWriteStream(writeStream);
        if (this.writeStream != null) {
            this.writeStream.setWriteQueueMaxSize(maxSize);
        }
        return this;
    }

    /**
     * Start the Pump. The Pump can be started and stopped multiple times.
     */
    public MultiReadPump add(ReadStream<Buffer> rs) {
        activeConnections.inc();
        rs.handler(dataHandler);
        readStreams.add(rs);
        if (this.writeStream == null) {
            rs.pause();
        }
        return this;
    }

    /**
     * Stop the Pump. The Pump can be started and stopped multiple times.
     */
    public MultiReadPump remove(ReadStream<?> rs) {
        activeConnections.dec();
        if (readStreams.remove(rs)) {
            rs.handler(null);
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
            requests.mark();
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



    private MultiReadPump() {

    }

}
