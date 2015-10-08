package io.cozmic.usher.plugins.kafka.highlevel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Properties;

/**
 * KafkaProducerConfig
 * Created by Craig Earley on 10/7/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaProducerConfig {

    public static final String KEY_KAFKA_TOPIC = "topic";
    public static final String KEY_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String KEY_KEY_SERIALIZER = "key.serializer";
    public static final String KEY_VALUE_SERIALIZER = "value.serializer";

    private final String kafkaTopic;
    private final String bootstrapServers;
    private final String keySerializer;
    private final String valueSerializer;

    private KafkaProducerConfig(String kafkaTopic, String bootstrapServers, String keySerializer, String valueSerializer) {
        this.kafkaTopic = kafkaTopic;
        this.bootstrapServers = bootstrapServers;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public static KafkaProducerConfig create(String kafkaTopic,
                                             String bootstrapServers,
                                             String keySerializer,
                                             String valueSerializer) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(bootstrapServers), "No configuration for key " + KEY_BOOTSTRAP_SERVERS);

        return new KafkaProducerConfig(kafkaTopic, bootstrapServers, keySerializer, valueSerializer);
    }

    public Properties getProperties() {
        Properties properties = new Properties();

        properties.put(KEY_KAFKA_TOPIC, getKafkaTopic());
        properties.put(KEY_BOOTSTRAP_SERVERS, getBootstrapServers());
        properties.put(KEY_KEY_SERIALIZER, getKeySerializer());
        properties.put(KEY_VALUE_SERIALIZER, getValueSerializer());

        return properties;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getKeySerializer() {
        return keySerializer;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }
}
