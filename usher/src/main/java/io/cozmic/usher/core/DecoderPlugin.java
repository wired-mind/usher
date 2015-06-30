package io.cozmic.usher.core;

import io.cozmic.usherprotocols.core.Message;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 6/25/15.
 */
public interface DecoderPlugin extends Plugin {
    void decode(Buffer record, Handler<Message> messageHandler);


}
