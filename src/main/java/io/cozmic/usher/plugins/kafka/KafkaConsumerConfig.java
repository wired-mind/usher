package io.cozmic.usher.plugins.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Properties;

/**
 * KafkaConsumerConfig
 * Created by Craig Earley on 10/7/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class KafkaConsumerConfig {

    public static final String KEY_GROUP_ID = "group.id";
    public static final String KEY_KAFKA_TOPIC = "topic";
    public static final String KEY_ZOOKEEPER = "zookeeper.connect";
    public static final String KEY_ZOOKEEPER_TIMEOUT_MS = "zookeeper.connection.timeout.ms";
    public static final String KEY_PARTITIONS = "partitions";

    private final String groupId;
    private final String kafkaTopic;
    private final String zookeeper;
    private final String zookeeperTimeout;
    private final int partitions;

    private KafkaConsumerConfig(String groupId, String kafkaTopic, String zookeeper, String zookeeperTimeout, int partitions) {
        this.groupId = groupId;
        this.kafkaTopic = kafkaTopic;
        this.zookeeper = zookeeper;
        this.partitions = partitions;
        this.zookeeperTimeout = zookeeperTimeout;
    }

    public static KafkaConsumerConfig create(String groupId,
                                             String kafkaTopic,
                                             String zookeeper,
                                             String zookeeperTimeout,
                                             int partitions) throws IllegalArgumentException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(groupId), "No configuration for key " + KEY_GROUP_ID);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(kafkaTopic), "No configuration for key " + KEY_KAFKA_TOPIC);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(zookeeper), "No configuration for key " + KEY_ZOOKEEPER);
        Preconditions.checkArgument(partitions > 0, "No configuration for key " + KEY_PARTITIONS);

        return new KafkaConsumerConfig(groupId, kafkaTopic, zookeeper, zookeeperTimeout, partitions);
    }

    public Properties getProperties() {
        Properties properties = new Properties();

        properties.put(KEY_GROUP_ID, getGroupId());
        properties.put(KEY_KAFKA_TOPIC, getKafkaTopic());
        properties.put(KEY_ZOOKEEPER, getZookeeper());
        properties.put(KEY_PARTITIONS, getPartitions());

        if (getZookeeperTimeout() != null) properties.put(KEY_ZOOKEEPER_TIMEOUT_MS, getZookeeperTimeout());

        return properties;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public String getZookeeperTimeout() {
        return zookeeperTimeout;
    }

    public int getPartitions() {
        return partitions;
    }
}
