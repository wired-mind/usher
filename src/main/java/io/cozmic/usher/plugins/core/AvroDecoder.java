package io.cozmic.usher.plugins.core;

import java.io.IOException;
import java.net.URL;
import java.util.Objects;

import com.google.common.io.Resources;
import io.cozmic.usher.core.AvroMapper;

import io.cozmic.usher.core.DecoderPlugin;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

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

    private AvroMapper avroMapper;
    private Class<?> clazz;

    @Override
    public void decode(PipelinePack pack, Handler<PipelinePack> pipelinePackHandler) throws IOException {
        final Buffer buffer = pack.getMsgBytes();
        pack.setMessage(avroMapper.deserialize(clazz, buffer.getBytes()));
        pipelinePackHandler.handle(pack);
    }


    @Override
    public DecoderPlugin createNew() {
        final AvroDecoder avroDecoder = new AvroDecoder();
        avroDecoder.init(configObj, vertx, clazz, avroMapper);
        return avroDecoder;
    }

    private void init(JsonObject configObj, Vertx vertx, Class<?> clazz, AvroMapper avroMapper) {

        this.configObj = configObj;
        this.vertx = vertx;
        this.clazz = clazz;
        this.avroMapper = avroMapper;
    }

    @Override
    public void init(JsonObject configObj, Vertx vertx) throws UsherInitializationFailedException {
        this.configObj = configObj;
        this.vertx = vertx;
        final String className = configObj.getString("clazz");
        final String schema = configObj.getString("schema");
        Objects.requireNonNull(className, "clazz is required");
        Objects.requireNonNull(schema, "schema is required");
        try {
            clazz = Class.forName(className);
            final URL resource = Resources.getResource(schema);
            avroMapper = new AvroMapper(resource.openStream());
        } catch (ClassNotFoundException | IOException e) {
            throw new UsherInitializationFailedException("Error initializing avro schema", e);
        }

    }
}