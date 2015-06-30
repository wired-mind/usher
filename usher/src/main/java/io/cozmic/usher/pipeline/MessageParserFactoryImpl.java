package io.cozmic.usher.pipeline;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usher.core.SplitterPlugin;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;

/**
 * Created by chuck on 6/29/15.
 */
public class MessageParserFactoryImpl implements MessageParserFactory {
    private final JsonObject inputObj;

    public MessageParserFactoryImpl(JsonObject inputObj) {
        this.inputObj = inputObj;
    }

    private SplitterPlugin createSplitter() {
        return new TokenSplitter();
    }

    private DecoderPlugin createDecoder() {
        return null;
    }

    @Override
    public MessageParser createParser(ReadStream<Buffer> readStream) {
        SplitterPlugin splitterPlugin = createSplitter();
        DecoderPlugin decoderPlugin = createDecoder();
        return new MessageParserImpl(readStream, splitterPlugin, decoderPlugin);
    }


}
