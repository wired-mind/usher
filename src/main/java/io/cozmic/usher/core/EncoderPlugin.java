package io.cozmic.usher.core;

import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

import java.io.IOException;

/**
 * Created by chuck on 6/30/15.
 */
public interface EncoderPlugin extends Plugin {
    void encode(PipelinePack pipelinePack, Handler<Buffer> bufferHandler) throws IOException;
    EncoderPlugin createNew();
}
