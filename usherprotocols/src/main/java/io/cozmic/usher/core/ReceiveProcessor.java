package io.cozmic.usher.core;

/**
 * Created by chuck on 9/17/14.
 */
public interface ReceiveProcessor extends MessageProcessor {
    String getMessageId();
}
