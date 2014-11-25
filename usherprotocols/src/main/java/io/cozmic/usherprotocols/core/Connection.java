package io.cozmic.usherprotocols.core;

import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

import java.net.InetSocketAddress;

/**
 * Created by chuck on 11/24/14.
 */
public class Connection {
    private final String connectionId;
    private final long timestamp = System.currentTimeMillis();
    private final InetSocketAddress localAddress;
    private final InetSocketAddress remoteAddress;
    private long closeTimestamp;

    public Connection(NetSocket sock, String connectionId) {
        this.connectionId = connectionId;
        localAddress = sock.localAddress();
        remoteAddress = sock.remoteAddress();
    }

    public Buffer asBuffer() {
        Buffer buffer = new Buffer();
        buffer.appendString(connectionId);
        buffer.appendLong(timestamp);
        buffer.appendLong(closeTimestamp);
        buffer.appendInt(localAddress.getHostName().length());
        buffer.appendString(localAddress.getHostName());
        buffer.appendInt(localAddress.getPort());
        buffer.appendInt(remoteAddress.getHostName().length());
        buffer.appendString(remoteAddress.getHostName());
        buffer.appendInt(remoteAddress.getPort());
        return buffer;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public void setCloseTimestamp(long closeTimestamp) {
        this.closeTimestamp = closeTimestamp;
    }
}
