package io.cozmic.usher.core;

import io.cozmic.usher.message.Message;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public interface MessageFilter extends WriteStream<Message> {
    WriteStream<Buffer> getInnerWriteStream();
}
