package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * Created by chuck on 7/10/15.
 */
public class AvroEncoder implements EncoderPlugin {
    private JsonObject configObj;
    private Vertx vertx;

    @Override
    public void encode(PipelinePack pipelinePack, Handler<Buffer> bufferHandler) {
        throw new RuntimeException("Avro Encoder not implemented yet");
    }

    @Override
    public EncoderPlugin createNew() {
        final AvroEncoder avroEncoder = new AvroEncoder();
        avroEncoder.init(configObj, vertx);
        return avroEncoder;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }
}
