package io.cozmic.usher.core;

import io.vertx.core.Handler;

/**
 * Created by chuck on 6/30/15.
 */
public interface Channel {
    Channel start();

    Channel endHandler(Handler<Void> endHandler);
}
