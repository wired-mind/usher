package io.cozmic.usher.streams;


import io.cozmic.usher.message.Message;
import io.cozmic.usher.core.MessageFilter;
import io.cozmic.usher.core.MessageParser;

/**
 * Created by chuck on 6/30/15.
 */
public class MessageStream extends DuplexStream<Message, Message> {
    private final MessageParser messageParser;
    private final MessageFilter messageFilter;

    public MessageStream(MessageParser messageParser, MessageFilter messageFilter) {
        super(messageParser, messageFilter);
        this.messageParser = messageParser;
        this.messageFilter = messageFilter;
    }

    public MessageParser getMessageParser() {
        return messageParser;
    }

    public MessageFilter getMessageFilter() {
        return messageFilter;
    }



}
