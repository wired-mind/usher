package io.cozmic.usherprotocols.core;


import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;

/**
 * Created by chuck on 11/24/14.
 */
public class Connection {
    private final String connectionId;
    private final long timestamp = System.currentTimeMillis();
    private final SocketAddress localAddress;
    private final SocketAddress remoteAddress;
    private long closeTimestamp;

    public Connection(NetSocket sock, String connectionId) {
        this.connectionId = connectionId;
        localAddress = sock.localAddress();
        remoteAddress = sock.remoteAddress();
    }

    public Buffer asBuffer() {
        Buffer buffer = Buffer.buffer();
        buffer.appendString(connectionId);
        buffer.appendLong(timestamp);
        buffer.appendLong(closeTimestamp);
        buffer.appendInt(localAddress.host().length());
        buffer.appendString(localAddress.host());
        buffer.appendInt(localAddress.port());
        buffer.appendInt(remoteAddress.host().length());
        buffer.appendString(remoteAddress.host());
        buffer.appendInt(remoteAddress.port());
        return buffer;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public void setCloseTimestamp(long closeTimestamp) {
        this.closeTimestamp = closeTimestamp;
    }
}
