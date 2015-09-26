package io.cozmic.usher.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroModule;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;

public class AvroMapper extends ObjectMapper {


    private AvroSchema avroSchema;
    public AvroMapper(File schemaFile) throws IOException {
        super(new AvroFactory());
        registerModule(new AvroModule());
        final Schema schema = new Schema.Parser().parse(schemaFile);
        avroSchema = new AvroSchema(schema);

    }

    public AvroMapper(URL url) throws IOException {
        this(url.openStream());
    }

    public AvroMapper(InputStream inputStream) throws IOException {
        super(new AvroFactory());
        registerModule(new AvroModule());
        Objects.requireNonNull(inputStream, "Schema doesn't exist.");
        Schema schema = new Schema.Parser().parse(inputStream);
        avroSchema = new AvroSchema(schema);
    }

    public AvroMapper(String s, String... more) {
        super(new AvroFactory());
        registerModule(new AvroModule());
        Schema schema = new Schema.Parser().parse(s, more);
        avroSchema = new AvroSchema(schema);
    }



    public AvroMapper(String s) {
        super(new AvroFactory());
        registerModule(new AvroModule());
        Schema schema = new Schema.Parser().parse(s);
        avroSchema = new AvroSchema(schema);
    }


    public byte[] serialize(Object obj) throws JsonProcessingException {
        return writer(avroSchema).writeValueAsBytes(obj);
    }

    public <T> T deserialize(Class clazz, byte[] bytes) throws IOException {
        final ObjectReader objectReader = reader(clazz).with(avroSchema);
        return objectReader.readValue(bytes);
    }

}
