package io.cozmic.usher.core;

import io.cozmic.usher.message.Message;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 6/30/15.
 */
public interface EncoderPlugin extends Plugin {
    void encode(Message message, Handler<Buffer> bufferHandler);
    EncoderPlugin createNew();
}
