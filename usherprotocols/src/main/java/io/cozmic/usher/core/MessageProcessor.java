package io.cozmic.usher.core;

import org.vertx.java.core.Handler;

/**
 * Created by chuck on 9/12/14.
 */
public interface MessageProcessor extends Handler<Message> {
    void setOutput(Handler<Message> output);
}
