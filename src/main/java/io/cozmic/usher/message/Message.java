package io.cozmic.usher.message;


import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.SocketAddress;
import io.vertx.core.shareddata.Shareable;

import java.util.UUID;

/**
 * Created by chuck on 9/29/14.
 */
public class Message implements Shareable {
    private UUID messageId;
    private Buffer payload;
    private String remoteHost;
    private int remotePort;
    private String localHost;
    private int localPort;

    private long timestamp;
    private String pluginName;


    public Buffer getPayload() {
        return payload;
    }

    public void setPayload(Buffer payload) {
        this.payload = payload;
    }


    public UUID getMessageId() {
        return messageId;
    }

    public void setMessageId(UUID messageId) {
        this.messageId = messageId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public void setRemoteHost(String remoteHost) {
        this.remoteHost = remoteHost;
    }

    public int getRemotePort() {
        return remotePort;
    }

    public void setRemotePort(int remotePort) {
        this.remotePort = remotePort;
    }

    public String getLocalHost() {
        return localHost;
    }

    public void setLocalHost(String localHost) {
        this.localHost = localHost;
    }

    public int getLocalPort() {
        return localPort;
    }

    public void setLocalPort(int localPort) {
        this.localPort = localPort;
    }

    public void setRemoteAddress(SocketAddress socketAddress) {
        remoteHost = socketAddress.host();
        remotePort = socketAddress.port();
    }

    public void setLocalAddress(SocketAddress socketAddress) {
        localHost = socketAddress.host();
        localPort = socketAddress.port();
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public String getPluginName() {
        return pluginName;
    }
}
