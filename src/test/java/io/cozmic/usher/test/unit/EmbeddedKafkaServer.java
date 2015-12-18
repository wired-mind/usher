package io.cozmic.usher.test.unit;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.rx.java.ObservableFuture;
import io.vertx.rx.java.RxHelper;
import kafka.admin.AdminUtils;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import rx.Observable;
import rx.functions.Func1;
import scala.Option;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * EmbeddedKafkaServer
 * Created by Craig Earley on 12/14/15.
 * Copyright (c) 2015 All Rights Reserved
 */
public class EmbeddedKafkaServer {
    public static final String SHUTDOWN_BROKER_REQUEST = "SHUTDOWN_BROKER_REQUEST";
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedKafkaServer.class.getName());
    private static final String defaultHost = "kafka.dev";
    private static final String defaultReplicationFactor = "1";
    private static final String defaultNumberOfPartitions = "1";

    private final int numberOfPartitions;
    private final int replicationFactor;
    private final EventBus eb;
    private final List<Integer> ports;
    private final Map<Integer, File> logDirs;
    private final Map<Integer, KafkaServer> brokers;
    private final Properties baseProperties;
    private final String host;
    private final String zkConnection;
    private final Vertx vertx;
    private final ZkClient zkClient;

    public EmbeddedKafkaServer(Vertx vertx, String zkConnection, Properties baseProperties, List<Integer> ports) {
        this(vertx, zkConnection, baseProperties, ports, defaultHost);
    }

    public EmbeddedKafkaServer(Vertx vertx, String zkConnection, Properties baseProperties, List<Integer> ports, String host) {
        this.vertx = vertx;
        this.eb = vertx.eventBus();
        this.zkConnection = zkConnection;
        this.baseProperties = baseProperties;
        this.ports = resolvePorts(ports);
        this.host = host;

        this.numberOfPartitions = Integer.parseInt(baseProperties.getProperty("num.partitions", defaultNumberOfPartitions));
        this.replicationFactor = Integer.parseInt(baseProperties.getProperty("offsets.topic.replication.factor", defaultReplicationFactor));

        this.brokers = new ConcurrentHashMap<>();
        this.logDirs = new ConcurrentHashMap<>();

        this.zkClient = new ZkClient(zkConnection, 30_000, 30_000, ZKStringSerializer$.MODULE$);
    }

    private static boolean deleteFile(File path) throws FileNotFoundException {
        if (!path.exists()) {
            throw new FileNotFoundException(path.getAbsolutePath());
        }
        boolean ret = true;
        if (path.isDirectory()) {
            File[] files = path.listFiles();
            if (files == null) {
                return true;
            }
            for (File f : files) {
                ret = ret && deleteFile(f);
            }
        }
        return ret && path.delete();
    }

    public void createTopic(String topic) {
        createTopic(topic, numberOfPartitions, replicationFactor);
    }

    public void createTopic(String topic, int numberOfPartitions, int replicationFactor) {
        if (!AdminUtils.topicExists(zkClient, topic)) {
            logger.info(String.format("Topic '%s' not found, creating...", topic));
            AdminUtils.createTopic(zkClient, topic, numberOfPartitions,
                    replicationFactor, new Properties());
            for (int i = 0; i < numberOfPartitions; i++) {
                TestUtils.waitUntilMetadataIsPropagated(JavaConversions.asScalaBuffer(getBrokers()), topic, i, 5000);
                TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, i, 500, Option.empty(), Option.empty());
            }
        } else {
            logger.info(String.format("Topic '%s' already exists", topic));
        }
    }

    public String getBootstrapServers() {
        if (brokers == null || brokers.size() == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (KafkaServer broker : brokers.values()) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(broker.config().hostName()).append(":").append(broker.config().port());
        }
        return sb.toString();
    }

    public List<KafkaServer> getBrokers() {
        return new ArrayList<>(brokers.values());
    }

    public Integer leadBroker(String topic, int partition) {
        Option<Object> leaderOpt = ZkUtils.getLeaderForPartition(zkClient, topic, partition);
        if (!leaderOpt.isDefined()) {
            logger.warn(String.format("Leader for topic %s partition %d does not exist", topic, partition));
        }
        return leaderOpt.isDefined() ? (Integer) leaderOpt.get() : null;
    }

    public String metaDataDump() {
        final StringBuilder sb = new StringBuilder();
        for (KafkaServer broker : brokers.values()) {
            kafka.javaapi.consumer.SimpleConsumer consumer = new SimpleConsumer(
                    broker.config().hostName(),
                    broker.config().port(),
                    100_000,
                    64 * 1024, this.getClass().getName());
            List<String> topics = new ArrayList<>();
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            try {
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                List<kafka.javaapi.TopicMetadata> data = resp.topicsMetadata();

                for (kafka.javaapi.TopicMetadata item : data) {
                    sb.append("Topic: ").append(item.topic()).append("\n");
                    for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata()) {
                        String replicas = "";
                        String isr = "";
                        for (kafka.cluster.Broker replica : part.replicas()) {
                            replicas += " " + replica;
                        }
                        for (kafka.cluster.Broker replica : part.isr()) {
                            isr += " " + replica;
                        }
                        sb.append("    Partition: ").append(part.partitionId()).append(": Leader: ").append(part.leader()).append(" Replicas:[").append(replicas).append(" ] ISR:[").append(isr).append(" ]").append("\n");
                    }
                }
                break;
            } catch (Exception e) {
                // continue for-loop
            }
        }
        return sb.toString();
    }

    public void onShutdownBroker(Integer id, Handler<AsyncResult<Void>> handler) {
        eb.consumer(SHUTDOWN_BROKER_REQUEST, message -> {
            if (message.body() != null && message.body().equals(id)) {
                logger.info("Received message to shutdown broker with id: " + id);
                shutdownBroker(id, handler);
            }
        });
    }

    public Observable<Void> restartCluster() {
        Observable<Void> observable = shutdownCluster();
        observable.flatMap((Func1<Void, Observable<?>>) aVoid -> start());
        return observable;
    }

    public Observable<Void> shutdownBroker(int id) {
        final ObservableFuture<Void> future = RxHelper.observableFuture();
        shutdownBroker(id, asyncResult -> future.toHandler().handle(asyncResult));
        return future;
    }

    public void shutdownBroker(int id, Handler<AsyncResult<Void>> asyncResult) {
        vertx.executeBlocking(future -> {
            try {
                brokers.get(id).shutdown();
                brokers.remove(id);
            } catch (Exception e) {
                logger.error(e);
            }
            try {
                deleteFile(logDirs.get(id));
                logDirs.remove(id);
            } catch (FileNotFoundException e) {
                logger.error(e);
            }
            future.complete();
        }, asyncResult);
    }

    public Observable<Void> shutdownCluster() {
        final ObservableFuture<Void> future = RxHelper.observableFuture();
        shutdownCluster(asyncResult -> future.toHandler().handle(asyncResult));
        return future;
    }

    public void shutdownCluster(Handler<AsyncResult<Void>> asyncResult) {
        vertx.executeBlocking(future -> {
            for (Integer id : brokers.keySet()) {
                try {
                    brokers.get(id).shutdown();
                    brokers.remove(id);
                } catch (Exception e) {
                    logger.error(e);
                }
            }
            for (Integer id : logDirs.keySet()) {
                try {
                    deleteFile(logDirs.get(id));
                    logDirs.remove(id);
                } catch (FileNotFoundException e) {
                    logger.error(e);
                }
            }
            zkClient.close();
            future.complete();
        }, asyncResult);
    }

    public Observable<Void> start() {
        final ObservableFuture<Void> future = RxHelper.observableFuture();
        start(asyncResult -> future.toHandler().handle(asyncResult));
        return future;
    }

    public void start(Handler<AsyncResult<Void>> asyncResult) {
        vertx.executeBlocking(future -> {
            for (int i = 0; i < ports.size(); i++) {
                Integer port = ports.get(i);
                File logDir = kafka.utils.TestUtils.tempDir();

                int brokerId = i + 1;

                Properties properties = new Properties();
                properties.putAll(baseProperties);
                properties.setProperty("zookeeper.connect", zkConnection);
                properties.setProperty("broker.id", String.valueOf(brokerId));
                properties.setProperty("host.name", host);
                properties.setProperty("port", Integer.toString(port));
                properties.setProperty("log.dir", logDir.getAbsolutePath());
                properties.setProperty("log.flush.interval.messages", String.valueOf(1));
                properties.put("replica.socket.timeout.ms", "1500");
                //properties.put("controlled.shutdown.enable", Boolean.toString(true));

                KafkaServer server = new KafkaServer(new KafkaConfig(properties), new MockTime());
                server.startup();

                brokers.put(brokerId, server);
                logDirs.put(brokerId, logDir);
            }
            future.complete();
        }, asyncResult);
    }

    @Override
    public String toString() {
        return "EmbeddedKafkaServer{" +
                "baseProperties=" + baseProperties +
                ", brokerList='" + getBootstrapServers() + '\'' +
                ", ports=" + ports +
                ", zkConnection='" + zkConnection + '\'' +
                '}';
    }

    public ZkClient zkClient() {
        return zkClient;
    }

    private int resolvePort(int port) {
        if (port == -1) {
            return kafka.utils.TestUtils.choosePort();
        }
        return port;
    }

    private List<Integer> resolvePorts(List<Integer> ports) {
        return ports.stream()
                .map(this::resolvePort).collect(Collectors.toList());
    }
}
