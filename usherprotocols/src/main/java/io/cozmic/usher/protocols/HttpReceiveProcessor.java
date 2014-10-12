package io.cozmic.usher.protocols;

import io.cozmic.usher.core.Message;
import io.cozmic.usher.core.ReceiveProcessor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;

/**
 * Created by chuck on 9/17/14.
 */
public class HttpReceiveProcessor implements ReceiveProcessor {
    private final HttpMessageProcessor httpRecordProcessor = new HttpMessageProcessor();
    private Handler<Message> output;
    private String messageId;

    @Override
    public void setOutput(Handler<Message> output) {
        this.output = output;
    }


    @Override
    public String getMessageId() {
        return messageId;
    }

    private Handler<Message> getOutput() {
        return output;
    }

    @Override
    public void handle(Message message) {
        httpRecordProcessor.setOutput(new Handler<Message>() {
            @Override
            public void handle(Message message) {
                messageId = "TODO: read from header";
                getOutput().handle(new Message("CHANGEME", message.getData()));
            }
        });
        httpRecordProcessor.handle(message);
    }
}
