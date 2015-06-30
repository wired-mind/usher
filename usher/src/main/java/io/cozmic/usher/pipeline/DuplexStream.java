package io.cozmic.usher.pipeline;

import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public class DuplexStream<R, W> {
    private final ReadStream<R> readStream;
    private final WriteStream<W> writeStream;

    public DuplexStream(ReadStream<R> readStream, WriteStream<W> writeStream) {
        this.readStream = readStream;
        this.writeStream = writeStream;
    }

    public WriteStream<W> getWriteStream() {
        return writeStream;
    }

    public ReadStream<R> getReadStream() {
        return readStream;
    }
}
