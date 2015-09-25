package io.cozmic.usher.core;


import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.cozmic.usher.plugins.core.UsherInitializationFailedException;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

import java.io.IOException;

/**
 * Created by chuck on 6/25/15.
 */
public interface DecoderPlugin extends Plugin {
    void decode(PipelinePack pack, Handler<PipelinePack> pipelinePackHandler) throws IOException;


    DecoderPlugin createNew();
}
