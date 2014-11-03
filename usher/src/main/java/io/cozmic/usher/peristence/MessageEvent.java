package io.cozmic.usher.peristence;

import io.cozmic.usherprotocols.core.Message;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

/**
 * Created by chuck on 9/15/14.
 */
public class MessageEvent {
    private Message message;

    public void setMessage(Message value) {
        this.message = value;
    }
    public Message getMessage() {
        return message;
    }

}
