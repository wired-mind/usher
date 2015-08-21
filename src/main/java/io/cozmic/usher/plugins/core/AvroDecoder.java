package io.cozmic.usher.plugins.core;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

/**
 * Decode serialized data to GenericRecord.
 * <p>
 * Created by Craig Earley on 8/19/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class AvroDecoder implements DecoderPlugin {
    Logger logger = LoggerFactory.getLogger(AvroDecoder.class.getName());

    private JsonObject configObj;
    private Vertx vertx;
    private Schema schema;

    @Override
    public void decode(PipelinePack pack, Handler<PipelinePack> pipelinePackHandler) {
        final Message message = pack.getMessage();
        final Buffer buffer = message.getPayload();

        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

        GenericRecord genericRecord = new GenericData.Record(schema);
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(buffer.getBytes(), null);
            genericRecord = reader.read(genericRecord, decoder);
        } catch (Exception e) {
            logger.error("Cannot decode data", e);
        }

        pack.setMessage(genericRecord);
        pipelinePackHandler.handle(pack);
    }

    @Override
    public DecoderPlugin createNew() {
        final AvroDecoder avroDecoder = new AvroDecoder();
        avroDecoder.init(configObj, vertx);
        return avroDecoder;
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
