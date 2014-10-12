package io.cozmic.usher.protocols;

import io.cozmic.usher.core.Message;
import io.cozmic.usher.core.MessageProcessor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.parsetools.RecordParser;

/**
 * Created by chuck on 9/12/14.
 */
public class HttpMessageProcessor implements MessageProcessor {

    public static final String HTTP_DELIM = "\r\n\r\n";
    private Handler<Message> output;
    private final RecordParser rawHttpParser = RecordParser.newDelimited(HTTP_DELIM, new Handler<Buffer>() {
        @Override
        public void handle(Buffer reqBuffer) {
            getOutput().handle(new Message(reqBuffer.appendString(HTTP_DELIM)));
        }
    });

    @Override
    public void setOutput(Handler<Message> output) {
        this.output = output;
    }

    @Override
    public void handle(final Message message) {
        rawHttpParser.handle(message.getData());
    }


    private Handler<Message> getOutput() {
        return output;
    }
}
