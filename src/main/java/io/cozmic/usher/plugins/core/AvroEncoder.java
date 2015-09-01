package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import com.fasterxml.jackson.dataformat.avro.AvroMapper;

import java.io.IOException;

/**
 * Encode GenericRecord to serialized data
 * <p>
 * Created by chuck on 7/10/15.
 */
public class AvroEncoder<T> implements EncoderPlugin {
    Logger logger = LoggerFactory.getLogger(AvroEncoder.class.getName());

    private JsonObject configObj;
    private Vertx vertx;
    private AvroMapper mapper = new AvroMapper();
    
    @Override
    public void encode(PipelinePack pipelinePack, Handler<Buffer> bufferHandler) {
        final T record = pipelinePack.getMessage();
        Buffer buffer = null;
        try {
        	buffer = Buffer.buffer(
        			mapper.writer(mapper.schemaFor(record.getClass())).writeValueAsBytes(record));
        } catch (IOException e) {
            logger.error("Cannot encode data", e);
        }

        bufferHandler.handle(buffer);
    }

    @Override
    public EncoderPlugin createNew() {
        final AvroEncoder<T> avroEncoder = new AvroEncoder<T>();
        avroEncoder.init(configObj, vertx);
        return avroEncoder;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {
        this.configObj = configObj;
        this.vertx = vertx;
    }
}
