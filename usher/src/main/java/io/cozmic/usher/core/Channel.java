package io.cozmic.usher.core;

import io.cozmic.usher.streams.MessageStream;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;

import java.util.List;

/**
 * Created by chuck on 6/30/15.
 */
public interface Channel {
    Channel start();
}
