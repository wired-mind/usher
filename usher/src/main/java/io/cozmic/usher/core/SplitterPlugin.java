package io.cozmic.usher.core;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 6/25/15.
 */
public interface SplitterPlugin extends Plugin {
    void findRecord(Buffer buffer, Handler<Buffer> bufferHandler);

}
