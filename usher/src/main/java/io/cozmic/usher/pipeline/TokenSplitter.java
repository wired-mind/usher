package io.cozmic.usher.pipeline;

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


    @Override
    public void init(JsonObject configObj, Vertx vertx) {
        rawParser = RecordParser.newDelimited(configObj.getString("token", HTTP_DELIM), null);
    }


    @Override
    public void findRecord(Buffer buffer, Handler<Buffer> bufferHandler) {
        rawParser.setOutput(bufferHandler);
        rawParser.handle(buffer);
    }
}
