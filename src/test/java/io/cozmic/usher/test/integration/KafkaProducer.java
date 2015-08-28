package io.cozmic.usher.test.integration;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.streams.Pump;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Enumeration;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * KafkaProducer
 * Created by Craig Earley on 8/27/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaProducer extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);

    public static final String KAFKA_BROKERS = "metadata.broker.list";
    public static final String KAFKA_CONFIG = "kafkaConfig";
    public static final String TCP_HOST = "tcpHost";
    public static final String TCP_PORT = "tcpPort";
    public static final String TOPIC = "topic";

    private int tcpPort;
    private Producer<String, String> kafkaProducer;
    private String tcpHost;
    private String topic;

    @Override
    public void start() throws Exception {
        tcpPort = config().getInteger(TCP_PORT, 1234);
        tcpHost = config().getString(TCP_HOST, NetServerOptions.DEFAULT_HOST);
        topic = config().getString(TOPIC, "default_topic");

        Properties kafkaProducerProps = new Properties();
        try {
            ResourceBundle kafkaProducerBundle = ResourceBundle.getBundle(KafkaProducer.KAFKA_CONFIG);

            Enumeration<String> keys = kafkaProducerBundle.getKeys();
            while (keys.hasMoreElements()) {
                String key = keys.nextElement();
                kafkaProducerProps.put(key, kafkaProducerBundle.getString(key));
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        if (config().containsKey(KAFKA_BROKERS)) {
            kafkaProducerProps.put(KAFKA_BROKERS, config().getString(KAFKA_BROKERS));
        }

        ProducerConfig kafkaProducerConfig = new ProducerConfig(kafkaProducerProps);
        kafkaProducer = new Producer<>(kafkaProducerConfig);

        NetServer server = vertx.createNetServer(
                new NetServerOptions().setPort(tcpPort).setHost(tcpHost)
        );
        server.connectHandler(sock -> {
            Pump.pump(sock, sock).start();
            sock.handler(buffer -> {
                String str = buffer.toString().trim();
                LOG.info("Sending message '" + str + "' to Kafka topic '" + topic + "'");
                sendMessage(topic, str);
                sock.write(buffer);
            });
        }).listen();

        LOG.info("KafkaProducer tcp server is now listening");
    }

    @Override
    public void stop() throws Exception {
        kafkaProducer.close();
    }

    /**
     * Send a message to Kafka using pattern as found at
     * https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example#
     */
    private void sendMessage(String topicName, String message) {
        KeyedMessage<String, String> data = new KeyedMessage<>(topicName, message);
        kafkaProducer.send(data);
    }
}
