package io.cozmic.usher.core;

import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.streams.MessageStream;
import io.cozmic.usher.streams.WriteCompleteFuture;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

/**
 * Given a stream of data, create one or more outgoing streams (demux). Can also be bidirectional. Responses from each
 * outgoing stream can be turned back into a single stream (mux)
 */
public interface StreamMux extends WriteStream<PipelinePack>, ReadStream<PipelinePack> {
    @Override
    StreamMux exceptionHandler(Handler<Throwable> handler);


    /**
     * Creates a MuxRegistration which adds an outgoing stream to to the demux. The response stream is included in the mux.
     * The registration will pump messages to the stream's MessageFilter and from the stream's MessageParser.
     * @param messageStream
     * @param bidirectional If bidirectional is true, then allow the stream's parser to be used as the responseStream
     * @return
     */
    MuxRegistration addStream(MessageStream messageStream, boolean bidirectional);

    void unregisterAllConsumers();


    /**
     * A special write method that will acknowledge when the underlying write is complete.
     * @param pack
     * @param doneHandler
     */
    StreamMux write(PipelinePack pack, Handler<AsyncResult<PipelinePack>> doneHandler);

    StreamMux writeCompleteHandler(Handler<WriteCompleteFuture> writeCompleteHandler);
}
