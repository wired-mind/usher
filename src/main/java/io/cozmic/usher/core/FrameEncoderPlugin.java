package io.cozmic.usher.core;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 7/10/15.
 */
public interface FrameEncoderPlugin extends Plugin {
    void encodeAndWrite(Buffer buffer);

    void setWriteHandler(Handler<Buffer> writeHandler);

    FrameEncoderPlugin createNew();
}
