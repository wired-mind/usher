package io.cozmic.usher.plugins.core;

import com.google.common.io.Resources;
import io.cozmic.usher.core.AvroMapper;
import io.cozmic.usher.core.EncoderPlugin;
import io.cozmic.usher.message.PipelinePack;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;



import java.io.IOException;
import java.net.URL;
import java.util.Objects;

/**
 * Encode GenericRecord to serialized data
 * <p>
 * Created by chuck on 7/10/15.
 */
public class AvroEncoder implements EncoderPlugin {
    Logger logger = LoggerFactory.getLogger(AvroEncoder.class.getName());

    private JsonObject configObj;
    private Vertx vertx;
    private Class<?> clazz;
    private AvroMapper avroMapper;


    @Override
    public void encode(PipelinePack pipelinePack, Handler<Buffer> bufferHandler) throws IOException {
        final Object record = pipelinePack.getMessage();
        final byte[] bytes = avroMapper.serialize(record);
        bufferHandler.handle(Buffer.buffer(bytes));
    }

    @Override
    public EncoderPlugin createNew() {
        final AvroEncoder avroEncoder = new AvroEncoder();
        avroEncoder.init(configObj, vertx, clazz, avroMapper);
        return avroEncoder;
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
