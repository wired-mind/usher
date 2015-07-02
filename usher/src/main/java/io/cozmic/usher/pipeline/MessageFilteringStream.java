package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.MessageFilter;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.streams.DuplexStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

/**
 * Created by chuck on 6/30/15.
 */
public class MessageFilteringStream extends DuplexStream<Buffer, Message> {
    private final ReadStream<Buffer> readStream;
    private final MessageFilter messageFilter;

    public MessageFilteringStream(ReadStream<Buffer> readStream, MessageFilter messageFilter) {
        super(readStream, messageFilter);
        this.readStream = readStream;
        this.messageFilter = messageFilter;
    }

    public MessageFilter getMessageFilter() {
        return messageFilter;
    }
}
