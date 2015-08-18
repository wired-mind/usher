package io.cozmic.usher.vertx.parsers;

import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.buffer.Buffer;

/**
 * Created by chuck on 8/18/15.
 */
public class PacketParsingException extends RuntimeException {
    private final String message;
    private final short invalidFormatCode;

    public PacketParsingException(String message, short invalidFormatCode) {
        super(message);
        this.message = message;
        this.invalidFormatCode = invalidFormatCode;
    }

    public PipelinePack getPipelinePack() {
        final Message message = new Message();
        final Buffer buffer = Buffer.buffer();
        buffer.setShort(0, invalidFormatCode);
        message.setPayload(buffer);
        final PipelinePack errorPack = new PipelinePack(message);
        return errorPack;
    }
}
