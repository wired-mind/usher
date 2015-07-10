package io.cozmic.usher.plugins.v1protocol;

import io.cozmic.usher.core.FrameEncoderPlugin;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

import java.util.Objects;

/**
 * Simple usher 1.0 compatible for right now.
 * We can create slightly more sophisticated frames when needed (https://hekad.readthedocs.org/en/v0.9.2/message/index.html)
 */
public class UsherV1FrameEncoder implements FrameEncoderPlugin {
    private Handler<Buffer> writeHandler;
    private JsonObject configObj;
    private Vertx vertx;


    @Override
    public void encodeAndWrite(Buffer buffer) {
        Objects.requireNonNull(writeHandler, "Write handler not set.");
        int messageLength = 4 + buffer.length();
        final Buffer frame = Buffer.buffer(messageLength);
        frame.appendInt(messageLength);
        frame.appendBuffer(buffer);
        writeHandler.handle(frame);
    }


    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }

    @Override
    public void setWriteHandler(Handler<Buffer> writeHandler) {
        this.writeHandler = writeHandler;
    }

    @Override
    public FrameEncoderPlugin createNew() {
        final UsherV1FrameEncoder usherV1FrameEncoder = new UsherV1FrameEncoder();
        usherV1FrameEncoder.init(configObj, vertx);
        return usherV1FrameEncoder;
    }
}
