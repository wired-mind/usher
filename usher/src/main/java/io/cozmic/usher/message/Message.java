package io.cozmic.usher.message;


import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.shareddata.Shareable;

import java.util.UUID;

/**
 * Created by chuck on 9/29/14.
 */
public class Message implements Shareable {
    private String messageId;
    private Buffer body;
    private SocketAddress remoteAddress;
    private SocketAddress localAddress;


    public Message(Buffer body) {
        this.body = body;
    }

    public Message(String messageId, Buffer body) {
        this.messageId = messageId;
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

    public static Message fromEnvelope(Buffer envelope) {
        int pos = 0;
        final int messageLength = envelope.getInt(pos);
        pos += 4;
        final int messageIdLength = envelope.getInt(pos);
        pos += 4;
        String messageId = envelope.getString(pos, pos + messageIdLength);
        pos += messageIdLength;

        final Buffer body = envelope.getBuffer(pos, envelope.length());
        return new Message(messageId, body);
    }

    public Message createReply(Buffer buffer) {
        Buffer reply = Buffer.buffer();
        reply.appendInt(messageId.length());
        reply.appendString(messageId);
        reply.appendBuffer(buffer);
        return new Message(messageId, reply);
    }


    public void setRemoteAddress(SocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public void setLocalAddress(SocketAddress localAddress) {
        this.localAddress = localAddress;
    }

    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    public SocketAddress getLocalAddress() {
        return localAddress;
    }
}
