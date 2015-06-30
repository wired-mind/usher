package io.cozmic.usher.core;

import io.cozmic.usher.pipeline.DuplexStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/25/15.
 */
public interface OutputPlugin extends Plugin {
    void run(AsyncResultHandler<DuplexStream<Buffer, Buffer>> duplexStreamAsyncResultHandler);

    void stop(WriteStream<Buffer> innerWriteStream);
}
