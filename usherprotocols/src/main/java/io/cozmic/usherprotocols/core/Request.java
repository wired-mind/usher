package io.cozmic.usherprotocols.core;

import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.shareddata.Shareable;

import java.util.UUID;

/**
 * Created by chuck on 9/29/14.
 */
public class Request implements Shareable{
    private String messageId;
    private String connectionId;
    private Long timestamp;
    private Buffer body;


    public Request(Buffer body) {
        this.body = body;
    }

    public Request(String messageId, String connectionId, long timestamp, Buffer body) {
        this.messageId = messageId;
        this.connectionId = connectionId;
        this.timestamp = timestamp;
        this.body = body;
    }

    public String getMessageId() {
        return messageId;
    }

    public Buffer getBody() {
        return body;
    }

    public String getOrCreateMessageId() {
        if (messageId == null) {
            messageId = UUID.randomUUID().toString();
        }
        return messageId;
    }

    public static Request fromEnvelope(Buffer envelope) {
        int pos = 0;
        final int messageLength = envelope.getInt(pos);
        pos += 4;
        final int messageIdLength = envelope.getInt(pos);
        pos += 4;
        String messageId = envelope.getString(pos, pos + messageIdLength);
        pos += messageIdLength;

        final int connectionIdLength = envelope.getInt(pos);
        pos += 4;
        String connectionId = envelope.getString(pos, pos + connectionIdLength);
        pos += connectionIdLength;

        final long timestamp = envelope.getLong(pos);
        pos += 8;

        final Buffer body = envelope.getBuffer(pos, envelope.length());
        return new Request(messageId, connectionId, timestamp, body);
    }

    public static Request fromEnvelope(byte[] bytes) {
        return fromEnvelope(new Buffer(bytes));
    }

    public Message createReply(Buffer buffer) {
        Buffer reply = new Buffer();
        reply.appendInt(messageId.length());
        reply.appendString(messageId);
        reply.appendBuffer(buffer);
        return new Message(messageId, reply);
    }

    public Buffer buildEnvelope() {
        int messageLength = 4 + 4  + messageId.length() + 4 + connectionId.length() + 8 + body.length();
        final Buffer envelope = new Buffer(messageLength);
        envelope.appendInt(messageLength);
        envelope.appendInt(messageId.length());
        envelope.appendString(messageId);
        envelope.appendInt(connectionId.length());
        envelope.appendString(connectionId);
        envelope.appendLong(timestamp);
        envelope.appendBuffer(body);
        return envelope;
    }

    public String getConnectionId() {
        return connectionId;
    }
}
