package io.cozmic.usher.core;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.ConcurrentHashSet;
import org.vertx.java.core.net.NetSocket;

/**
 * Created by chuck on 10/1/14.
 */
public class FilteredCozmicSocket extends CozmicSocket {

    private final ConcurrentHashSet<String> filters = new ConcurrentHashSet<>();

    public FilteredCozmicSocket(NetSocket sock) {
        super(sock, null);
    }

    @Override
    public CozmicSocket dataHandler(Handler<Buffer> handler) {
        return super.dataHandler(handler);
    }

}
