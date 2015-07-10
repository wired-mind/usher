package io.cozmic.usher.core;


import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 6/25/15.
 */
public interface DecoderPlugin extends Plugin {
    void decode(PipelinePack pack, Handler<PipelinePack> pipelinePackHandler);


    DecoderPlugin createNew();
}
