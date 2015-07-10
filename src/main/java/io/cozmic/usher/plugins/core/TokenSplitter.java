package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.SplitterPlugin;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;

/**
 * Created by chuck on 6/29/15.
 */
public class TokenSplitter implements SplitterPlugin {

    public static final String HTTP_DELIM = "\r\n\r\n";

    private RecordParser rawParser;
    private JsonObject configObj;
    private Vertx vertx;


    @Override
    public void init(JsonObject configObj, Vertx vertx) {
        this.configObj = configObj;
        this.vertx = vertx;
        rawParser = RecordParser.newDelimited(configObj.getString("delimiter", HTTP_DELIM), null);
    }


    @Override
    public void findRecord(Buffer buffer, Handler<Buffer> bufferHandler) {
        rawParser.setOutput(bufferHandler);
        rawParser.handle(buffer);
    }

    @Override
    public SplitterPlugin createNew() {
        final TokenSplitter tokenSplitter = new TokenSplitter();
        tokenSplitter.init(configObj, vertx);
        return tokenSplitter;
    }
}
