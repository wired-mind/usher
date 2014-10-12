package io.cozmic.usher.core;

import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.shareddata.Shareable;

import java.util.UUID;

/**
* Created by chuck on 9/29/14.
*/
public class Message implements Shareable{
    private String messageId;
    private Buffer data;
    private long timeoutId;
    private String replyAddress;
    private NetSocket socket;

    public Message(Buffer data) {
        this(data, 0, null);
    }

    public Message(Buffer data, long timeoutId, String replyAddress) {
        this.data = data;
        this.timeoutId = timeoutId;
        this.replyAddress = replyAddress;
    }

    public Message(String messageId, Buffer data) {
        this.messageId = messageId;
        this.data = data;
    }

    public String getMessageId() {
        return messageId;
    }

    public Buffer getData() {
        return data;
    }

    public long getTimeoutId() {
        return timeoutId;
    }

    public String getReplyAddress() {
        return replyAddress;
    }

    public void setData(Buffer data) {
        this.data = data;
    }

    public void setTimeoutId(long timeoutId) {
        this.timeoutId = timeoutId;
    }

    public void setReplyAddress(String replyAddress) {
        this.replyAddress = replyAddress;
    }

    public String getOrCreateMessageId() {
        if (messageId == null) {
            messageId = UUID.randomUUID().toString();
        }
        return messageId;
    }

    public void setSocket(NetSocket socket) {
        this.socket = socket;
    }

    public NetSocket getSocket() {
        return socket;
    }

    public static Message fromBuffer(Buffer buffer) {
        int pos = 0;
        final int messageIdLength = buffer.getInt(pos);
        pos += 4;
        String messageId = buffer.getString(pos, pos + messageIdLength);
        pos += messageIdLength;

        final Buffer body = buffer.getBuffer(pos, buffer.length());
        return new Message(messageId, body);
    }

    public Message createReply(Buffer buffer) {
        Buffer reply = new Buffer();
        reply.appendInt(messageId.length());
        reply.appendString(messageId);
        reply.appendBuffer(buffer);
        return new Message(messageId, reply);
    }
}
