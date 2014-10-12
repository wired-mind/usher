package io.cozmic.usher.core;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;

/**
 * Created by chuck on 9/29/14.
 */
public interface RecordProcessor extends Handler<Buffer> {
    void setOutput(Handler<Buffer> output);
}