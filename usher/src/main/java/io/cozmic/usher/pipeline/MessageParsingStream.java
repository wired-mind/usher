package io.cozmic.usher.pipeline;

import io.cozmic.usherprotocols.core.Message;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public class MessageParsingStream extends DuplexStream<Message, Buffer> {
    private final MessageParser messageParser;
    private final WriteStream<Buffer> writeStream;

    public MessageParsingStream(MessageParser messageParser, WriteStream<Buffer> writeStream) {
        super(messageParser, writeStream);
        this.messageParser = messageParser;
        this.writeStream = writeStream;
    }

    public MessageParser getMessageParser() {
        return messageParser;
    }
}
