package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Encode GenericRecord to serialized data
 * <p>
 * Created by chuck on 7/10/15.
 */
public class GenericAvroEncoder implements EncoderPlugin {
    Logger logger = LoggerFactory.getLogger(GenericAvroEncoder.class.getName());

    private JsonObject configObj;
    private Vertx vertx;
    private Schema schema;

    @Override
    public void encode(PipelinePack pipelinePack, Handler<Buffer> bufferHandler) {
        final GenericRecord record = pipelinePack.getMessage();

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            writer.write(record, encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            logger.error("Cannot encode data", e);
        }

        final Buffer buffer = Buffer.buffer(out.toByteArray());
        bufferHandler.handle(buffer);
    }

    @Override
    public EncoderPlugin createNew() {
        final GenericAvroEncoder avroEncoder = new GenericAvroEncoder();
        avroEncoder.init(configObj, vertx);
        return avroEncoder;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {
        this.configObj = configObj;
        this.vertx = vertx;
        if (configObj.getString("Schema") == null) {
            logger.fatal("No avro schema provided!");
        }
        this.schema = new Schema.Parser().parse(configObj.getString("Schema"));
    }
}
