package io.cozmic.usher.core;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.cozmic.usher.Start;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.ConcurrentHashSet;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;

import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by chuck on 10/2/14.
 */
public class MultiReadPump {

    private final Meter requests = Start.metrics.meter("requests");
    private final Counter activeConnections = Start.metrics.counter(name(MultiReadPump.class, "active-connections"));

    private final ConcurrentHashSet<ReadStream<?>> readStreams = new ConcurrentHashSet<>();
    private WriteStream<?> writeStream;
    private int pumped;


    public static MultiReadPump createPump() {
        return new MultiReadPump();
    }

    public int getConnectionCount() {
        return readStreams.size();
    }

    public MultiReadPump setWriteStream(WriteStream<?> writeStream) {
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
    public MultiReadPump setWriteStream(WriteStream<?> writeStream, int maxSize) {
        this.setWriteStream(writeStream);
        if (this.writeStream != null) {
            this.writeStream.setWriteQueueMaxSize(maxSize);
        }
        return this;
    }

    /**
     * Start the Pump. The Pump can be started and stopped multiple times.
     */
    public MultiReadPump add(ReadStream<?> rs) {
        activeConnections.inc();
        rs.dataHandler(dataHandler);
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
