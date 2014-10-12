package io.cozmic.usher.protocols;

import io.cozmic.usher.core.Message;
import io.cozmic.usher.core.MessageProcessor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;

/**
 * Created by chuck on 9/17/14.
 */
public class NetSendProcessor implements MessageProcessor {

    private Handler<Message> output;



    @Override
    public void setOutput(Handler<Message> output) {
        this.output = output;
    }

    @Override
    public void handle(Message message) {
        Buffer data = message.getData();
        final String messageId = message.getOrCreateMessageId();
        int messageLength = 4 + 4 + messageId.length() + data.length();
        final Buffer envelope = new Buffer(messageLength);
        envelope.appendInt(messageLength);
        envelope.appendInt(messageId.length());
        envelope.appendString(messageId);
        envelope.appendBuffer(data);

        message.setData(envelope);
        getOutput().handle(message);
    }

    private Handler<Message> getOutput() {
        return output;
    }
}