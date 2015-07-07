package io.cozmic.usher.core;

import io.cozmic.usher.message.Message;
import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

import java.util.List;

/**
 * Given a stream of data, create one or more outgoing streams
 */
public interface StreamDemultiplexer extends WriteStream<Message> {
    @Override
    StreamDemultiplexer exceptionHandler(Handler<Throwable> handler);


    /**
     * Creates a MessageConsumer which provides a readstream of messages. The consumer will pump messages to the stream's
     * MessageFilter.
     * @param messageStream
     * @param bidirectional If bidirectional is true, then allow the stream's parser to be used as the responseStream
     * @return
     */
    MessageConsumer consumer(MessageStream messageStream, boolean bidirectional);

    void unregisterAllConsumers();

    List<MessageParser> getResponseStreams();
}
