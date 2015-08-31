package io.cozmic.usher.test.integration;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.streams.Pump;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Enumeration;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * KafkaProducer
 * Created by Craig Earley on 8/27/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaProducer extends AbstractVerticle {

    public static final String KAFKA_BROKERS = "bootstrap.servers";
    public static final String KAFKA_CONFIG = "kafkaProducer";
    public static final String TCP_HOST = "tcpHost";
    public static final String TCP_PORT = "tcpPort";
    public static final String TOPIC = "topic";
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);
    private int tcpPort;
    private org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> kafkaProducer;
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

            if (config().containsKey(KAFKA_BROKERS)) {
                kafkaProducerProps.put(KAFKA_BROKERS, config().getString(KAFKA_BROKERS));
            }

            kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(kafkaProducerProps);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        NetServer server = vertx.createNetServer(
                new NetServerOptions().setPort(tcpPort).setHost(tcpHost)
        );
        server.connectHandler(sock -> {
            Pump.pump(sock, sock).start();
            sock.handler(buffer -> {
                byte[] bytes = buffer.getBytes();
                sendMessage(topic, "test-key", bytes);
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
    private void sendMessage(String topic, String key, byte[] value) {
        ProducerRecord<String, byte[]> data = new ProducerRecord<>(topic, key, value);
        kafkaProducer.send(data, (metadata, e) -> {
            if (e != null) {
                LOG.error("Error sending message: ", e);
                return;
            }
            LOG.info(String.format("Sent message: offset: %d, topic: %s, partition: %d",
                    metadata.offset(), metadata.topic(), metadata.partition()));
        });
    }
}
