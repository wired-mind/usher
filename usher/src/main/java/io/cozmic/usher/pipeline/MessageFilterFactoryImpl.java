package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.EncoderPlugin;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;

/**
 * Created by chuck on 6/30/15.
 */
public class MessageFilterFactoryImpl implements MessageFilterFactory {
    private final JsonObject outputObj;

    public MessageFilterFactoryImpl(JsonObject outputObj) {
        this.outputObj = outputObj;
    }
    @Override
    public MessageFilter createFilter(WriteStream<Buffer> writeStream){
        EncoderPlugin encoderPlugin = createEncoder();
        return new MessageFilterImpl(writeStream, encoderPlugin);
    }

    private EncoderPlugin createEncoder() {
        return null;
    }
}
