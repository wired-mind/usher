package io.cozmic.usher.journal;

import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

/**
 * Created by chuck on 9/15/14.
 */
public class JournalEvent {
    private Buffer buff;
    private NetSocket socket;

    public void setBuff(Buffer value) {
        this.buff = value;
    }
    public Buffer getBuff() {
        return buff;
    }

    public NetSocket getSocket() {
        return socket;
    }

    public void setSocket(NetSocket socket) {
        this.socket = socket;
    }
}
