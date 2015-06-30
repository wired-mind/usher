package io.cozmic.usherprotocols.parsing;


import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;

/**
 * Created by chuck on 11/13/14.
 */
public class CozmicParser implements Handler<Buffer> {

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
    public void handle(Buffer data) {
        parser.handle(data);
    }

    public void handler(Handler<Buffer> handler) {
        this.handler = handler;
    }
}
