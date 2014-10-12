package io.cozmic.usher.protocols;

import io.cozmic.usher.core.Message;
import io.cozmic.usher.core.ReceiveProcessor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.parsetools.RecordParser;

/**
 * Created by chuck on 9/17/14.
 */
public class NetReceiveProcessor implements ReceiveProcessor {
    public static final int LENGTH_HEADER_SIZE = 4;
    private Handler<Message> output;
    private String messageId;
    private final RecordParser parser = RecordParser.newFixed(LENGTH_HEADER_SIZE, null);
    private final Handler<Buffer> handler;

    public NetReceiveProcessor() {
        handler = new Handler<Buffer>() {
            int messageSize = -1;
            public void handle(Buffer buff) {
                if (messageSize == -1) {
                    messageSize = buff.getInt(0) - LENGTH_HEADER_SIZE;
                    parser.fixedSizeMode(messageSize);
                } else {
                    int pos = 0;
                    final int messageIdLength = buff.getInt(pos);
                    pos += 4;
                    messageId = buff.getString(pos, pos + messageIdLength);
                    pos += messageIdLength;

                    final Buffer body = buff.getBuffer(pos, messageSize);

                    parser.fixedSizeMode(LENGTH_HEADER_SIZE);
                    messageSize = -1;

                    getOutput().handle(new Message(messageId, body));
                }
            }
        };

        parser.setOutput(handler);
    }

    @Override
    public void setOutput(Handler<Message> output) {
        this.output = output;
    }

    @Override
    public void handle(Message message) {
        parser.handle(message.getData());
    }


    private Handler<Message> getOutput() {
        return output;
    }

    @Override
    public String getMessageId() {
        return messageId;
    }


}
