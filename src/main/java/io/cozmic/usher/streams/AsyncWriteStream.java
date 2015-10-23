package io.cozmic.usher.streams;

import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

/**
 * Specialized write stream that provides a way to signal when a write is complete.
 */
public interface AsyncWriteStream<T> extends WriteStream<T> {
    /**
     * Implementors will write the data to the underlying stream. Use the future to indicate when done.
     * @param data Data to write to stream
     * @param writeCompleteFuture Future to indicate the write is complete
     * @param pipelinePack The pipeline pack for context
     * @return Instance of self for chaining
     */
    public AsyncWriteStream<T> write(T data, Future<Void> writeCompleteFuture, PipelinePack pipelinePack);
}
