package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.MessageInjector;
import io.cozmic.usher.core.StreamMux;

/**
 * Created by chuck on 9/30/15.
 */
public class MessageInjectorImpl implements MessageInjector {
    private final StreamMux mux;

    public MessageInjectorImpl(StreamMux mux) {
        this.mux = mux;
    }
}
