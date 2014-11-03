package io.cozmic.usher.core;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Created by chuck on 10/24/14.
 */
public interface ClusterListener {
    ClusterListener listener(Listener listener);

    void start();

    Map<String,InetSocketAddress> getMembers();
}
