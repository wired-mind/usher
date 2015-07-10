package io.cozmic.usher.plugins.v1protocol;

import io.cozmic.usher.core.FramingSplitter;
import io.cozmic.usher.core.SplitterPlugin;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;

/**
 * Created by chuck on 7/10/15.
 */
public class UsherV1FramingSplitter implements SplitterPlugin, FramingSplitter {
    private JsonObject configObj;
    private Vertx vertx;

    private Handler<Buffer> handler;

    public static final int LENGTH_HEADER_SIZE = 4;
    private RecordParser parser = RecordParser.newFixed(LENGTH_HEADER_SIZE, new Handler<Buffer>() {
        int messageSize = -1;

        public void handle(Buffer buff) {

            if (messageSize == -1) {
                messageSize = buff.getInt(0) - LENGTH_HEADER_SIZE;
                parser.fixedSizeMode(messageSize);
            } else {
                parser.fixedSizeMode(LENGTH_HEADER_SIZE);
                messageSize = -1;

                if (handler != null) {
                    handler.handle(buff);
                }
            }
        }
    });


    @Override
    public void findRecord(Buffer buffer, Handler<Buffer> bufferHandler) {
        handler = bufferHandler;
        parser.handle(buffer);
    }

    @Override
    public SplitterPlugin createNew() {
        final UsherV1FramingSplitter usherV1FramingSplitter = new UsherV1FramingSplitter();
        usherV1FramingSplitter.init(configObj, vertx);
        return usherV1FramingSplitter;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }
}
