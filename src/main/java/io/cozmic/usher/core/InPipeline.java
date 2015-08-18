package io.cozmic.usher.core;

import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.streams.ReadStream;

/**
 * Created by chuck on 6/30/15.
 */
public interface InPipeline extends ReadStream<PipelinePack> {
    void close();
}
