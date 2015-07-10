package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

/**
 * Created by chuck on 7/1/15.
 */
public class CozmicEncoder implements EncoderPlugin {
    private JsonObject configObj;
    private Vertx vertx;

    @Override
    public void init(JsonObject configObj, Vertx vertx) {

        this.configObj = configObj;
        this.vertx = vertx;
    }


    /**
     * This is sort of a temporary shim until we sort out exactly how Message should work
     * @param pipelinePack
     * @param bufferHandler
     */
    @Override
    public void encode(PipelinePack pipelinePack, Handler<Buffer> bufferHandler) {
//        pipelinePack.getOrCreateMessageId();
//        bufferHandler.handle(buildEnvelope(pipelinePack, UUID.randomUUID().toString()));
    }

    @Override
    public EncoderPlugin createNew() {
        final CozmicEncoder cozmicEncoder = new CozmicEncoder();
        cozmicEncoder.init(configObj, vertx);
        return cozmicEncoder;
    }

    public Buffer buildEnvelope(Message message, String messageId) {
//        final Buffer body = message.getPayload();
//        int messageLength = 4 + 4 + messageId.length() + body.length();
//        final Buffer envelope = Buffer.buffer(messageLength);
//        envelope.appendInt(messageLength);
//        envelope.appendInt(messageId.length());
//        envelope.appendString(messageId);
//        envelope.appendBuffer(body);
//        return envelope;
        return null;
    }

}
