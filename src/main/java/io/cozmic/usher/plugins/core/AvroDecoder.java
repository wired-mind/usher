package io.cozmic.usher.plugins.core;

import java.io.IOException;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usher.message.Message;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;

/**
 * Decode serialized data to GenericRecord.
 * <p>
 * Created by Craig Earley on 8/19/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class AvroDecoder<T> implements DecoderPlugin {
    Logger logger = LoggerFactory.getLogger(AvroDecoder.class.getName());

    private JsonObject configObj;
    private Vertx vertx;
    private ObjectMapper mapper = new ObjectMapper(new AvroFactory());
    private AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator();

    @Override
    public void decode(PipelinePack pack, Handler<PipelinePack> pipelinePackHandler) {
        final Message message = pack.getMessage();
        final Buffer buffer = message.getPayload();

        T record = null;
		try {
			Class<?> clazz = Class.forName(configObj.getJsonObject("avro").getString("type"));
            mapper.acceptJsonFormatVisitor(clazz, schemaGenerator);
			record = mapper.reader(clazz).with(schemaGenerator.getGeneratedSchema())
							.readValue(buffer.getBytes());
		} catch (JsonProcessingException e) {
			logger.error("Error deserializing pojo", e);
		} catch (IOException e) {
			logger.error("Error deserializing pojo", e);
		} catch (ClassNotFoundException e) {
			logger.error("Cannot find class of pojo", e);
		}
        pack.setMessage(record);
        pipelinePackHandler.handle(pack);
    }

    @Override
    public DecoderPlugin createNew() {
        final AvroDecoder<T> avroDecoder = new AvroDecoder<T>();
        avroDecoder.init(configObj, vertx);
        return avroDecoder;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) {
        this.configObj = configObj;
        this.vertx = vertx;
    }
}