package io.cozmic.usher.old;

import com.codahale.metrics.*;
import io.cozmic.usherprotocols.core.Connection;
import io.cozmic.usherprotocols.core.CozmicPump;
import io.cozmic.usherprotocols.core.MessageReadStream;
import io.cozmic.usherprotocols.core.Request;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.streams.ReadStream;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerConnector;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by chuck on 10/24/14.
 */
public abstract class ProxyTunnel {
    Logger logger = LoggerFactory.getLogger(ProxyTunnel.class.getName());

    public static final MetricRegistry metrics = new MetricRegistry();
    public static final int ONE_SECOND_IN_MILLIS = 1000;
    public static final int BUCKET_SIZE = ONE_SECOND_IN_MILLIS / 10;
    public static final int STARTING_BUCKET_ID = 1;
    private final Timer responses = metrics.timer(name(ProxyTunnel.class, "responses"));
    private final Timer clientConnects = metrics.timer(name(ProxyTunnel.class, "client-connects"));
    private final Timer serviceConnects = metrics.timer(name(ProxyTunnel.class, "service-connects"));
    private final Counter inflightMessages = metrics.counter(name(ProxyTunnel.class, "inflight-messages"));
    private final Meter timeouts = metrics.meter("timeouts");
    private final Meter clientErrors = metrics.meter("client-errors");
    private final Meter serviceErrors = metrics.meter("service-errors");
    private final Meter responsesAlreadyTimedout = metrics.meter("responses-already-timedout");
    private final ConcurrentHashMap<String, Timer.Context> inflightTimers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ConcurrentHashMap<String, Request>> timeoutBuckets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> messageTimeoutBucketMap = new ConcurrentHashMap<>();

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 2500;
    public static final int DEFAULT_TIMEOUT = 30000;
    public static final int ALWAYS_ATTEMPT_RECONNECT = -1;
    private final Vertx vertx;

    private NetServer netServer;
    private final NetClient netClient;
    private final String proxyHost;
    private final Integer proxyPort;
    private final Integer proxyTimeout;
    private ServiceGateway serviceGateway;

    static {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(10, TimeUnit.SECONDS);
    }

    public ProxyTunnel(Context container, final Vertx vertx) {
        this.vertx = vertx;

        final KafkaProducer<String, String> producer = new KafkaProducer<>(new Properties());
        producer.send(new ProducerRecord<String, String>("test", "test", "test"), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {

            }
        });

        final ConsumerConnector consumerConnector = kafka.consumer.Consumer.create(new ConsumerConfig(new Properties()));

        JsonObject config = new JsonObject();
        proxyHost = config.getString("host", DEFAULT_HOST);
        proxyPort = config.getInteger("port", DEFAULT_PORT);
        proxyTimeout = config.getInteger("timeout", DEFAULT_TIMEOUT);

        logger.info("[ProxyTunnel] Initializing");
        netClient = vertx.createNetClient();
//        netClient.setConnectTimeout(ONE_SECOND_IN_MILLIS * 10);
//        netClient.setReconnectAttempts(ALWAYS_ATTEMPT_RECONNECT);
//        netClient.setReconnectInterval(500);
//
        serviceGateway = new ServiceGateway();
//
//
        netServer = vertx.createNetServer();
//        netServer.setAcceptBacklog(10000);


        netServer.connectHandler(new Handler<NetSocket>() {
            @Override
            public void handle(final NetSocket sock) {
                final Timer.Context connectTimer = clientConnects.time();

                String connectionId = UUID.randomUUID().toString();
                final Connection connection = new Connection(sock, connectionId);
                serviceGateway.addSocketToPumps(sock, connection, connectTimer);

                sock.exceptionHandler(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable event) {
                        logger.error("[ProxyTunnel] Socket error", event);
                        connectTimer.stop();
                        clientErrors.mark();
                        connection.setCloseTimestamp(System.currentTimeMillis());
//                        connectionProducer.onData(connection);
                    }
                });

            }
        });
    }




    //Round to bucket size (Decasecond in this case)
    protected long currentTimeoutBucketId() {
        return (long) (Math.ceil(System.currentTimeMillis() / BUCKET_SIZE) * BUCKET_SIZE);
    }


    public void listen(final AsyncResultHandler<Void> handler) {
        netServer.listen(proxyPort, proxyHost, new AsyncResultHandler<NetServer>() {
            @Override
            public void handle(AsyncResult<NetServer> event) {
                if (!event.succeeded()) {
                    logger.error(event.cause());
                    handler.handle(Future.failedFuture(event.cause()));
                    return;
                }

                logger.info("[ProxyTunnel] Ready to accept connections");
                handler.handle(Future.succeededFuture());
            }
        });
    }

    public void connect(final Integer servicePort, final String serviceHost, final AsyncResultHandler<Void> readyHandler) {

        serviceGateway.disconnectedHandler(new Handler<Void>() {
            @Override
            public void handle(Void event) {
                logger.info("[ServiceGateway] disconnected. Closing [ProxyTunnel] listener.");
                netServer.close(new Handler<AsyncResult<Void>>() {
                    @Override
                    public void handle(AsyncResult<Void> event) {
                        if (!event.succeeded()) {
                            logger.error(event.cause());
                            return;
                        }

                        logger.info("[ProxyTunnel] closed after [ServiceGateway] disconnect. Attempting to reestablish connection.");
                        netClient.connect(servicePort, serviceHost, serviceGateway);
                    }
                });



            }
        });

        serviceGateway.connectedHandler(readyHandler);
        netClient.connect(servicePort, serviceHost, serviceGateway);
    }



    protected abstract MessageReadStream wrapWithMessageReader(NetSocket serviceSocket);

    protected abstract ReadStream<Buffer> wrapReadStream(NetSocket sock, Connection connection, CozmicPump receivePump);

    public String getProxyHost() {
        return proxyHost;
    }

    public Integer getProxyPort() {
        return proxyPort;
    }

    private class ServiceGateway implements Handler<AsyncResult<NetSocket>> {
        private Handler<Void> disconnectedHandler;
        private Handler<AsyncResult<Void>> connectedHandler;

        private MultiReadPump multiReadPump;
        private CozmicPump receivePump;

        public ServiceGateway() {

            multiReadPump = MultiReadPump.createPump();
            receivePump = CozmicPump.createPump();


            vertx.setPeriodic(BUCKET_SIZE, new Handler<Long>() {
                @Override
                public void handle(Long event) {
                    final long currentTimeMillis = System.currentTimeMillis();
                    for (Long timeoutBucketId : timeoutBuckets.keySet()) {
                        if (timeoutBucketId < currentTimeMillis - proxyTimeout) {
                            final ConcurrentHashMap<String, Request> timeoutBucket = timeoutBuckets.remove(timeoutBucketId);
                            for (String messageId : timeoutBucket.keySet()) {
                                final Request request = timeoutBucket.get(messageId);
                                receivePump.timeoutMessage(messageId);
//                                timeoutLogProducer.onData(request);
                                timeouts.mark();
                            }
                        }
                    }
                }
            });

        }

        public int getConnectionCount() {
            return multiReadPump.getConnectionCount();
        }

        @Override
        public void handle(AsyncResult<NetSocket> asyncResult) {
            if (asyncResult.failed()) {
                logger.error(asyncResult.cause());
                if (connectedHandler != null) {
                    connectedHandler.handle(Future.failedFuture(asyncResult.cause()));
                }
                return;
            }
            final Timer.Context connectTimer = serviceConnects.time();

            final NetSocket serviceSocket = asyncResult.result();

            final MessageReadStream messageReadStream = wrapWithMessageReader(serviceSocket);
//            final WriteStream<?> journalingServiceSocket = new JournalingWriteStream(serviceSocket, journalProducer);
            multiReadPump.setWriteStream(serviceSocket);
            receivePump.setMessageReadStream(messageReadStream);
            receivePump.inflightHandler(new Handler<Request>() {
                @Override
                public void handle(final Request request) {
                    inflightMessages.inc();
                    inflightTimers.put(request.getMessageId(), responses.time());
                    final long currentTimeoutBucketId = currentTimeoutBucketId();

                    if (!timeoutBuckets.containsKey(currentTimeoutBucketId)) {
                        timeoutBuckets.put(currentTimeoutBucketId, new ConcurrentHashMap<String, Request>());
                    }
                    final ConcurrentHashMap<String, Request> timeoutBucket = timeoutBuckets.get(currentTimeoutBucketId);
                    timeoutBucket.put(request.getMessageId(), request);
                    messageTimeoutBucketMap.put(request.getMessageId(), currentTimeoutBucketId);
                }
            });
            receivePump.responseHandler(new Handler<String>() {
                @Override
                public void handle(String messageId) {

                    removeMessageFromInflight(messageId);
                }
            });


            serviceSocket.exceptionHandler(new Handler<Throwable>() {
                @Override
                public void handle(Throwable event) {
                    logger.error("[ServiceGateway] socket error", event);
                    serviceErrors.mark();
                }
            });
            serviceSocket.closeHandler(new Handler<Void>() {
                @Override
                public void handle(Void event) {
                    logger.info("[ServiceGateway] socket closing");
                    multiReadPump.setWriteStream(null);
                    receivePump.setMessageReadStream(null);
                    if (disconnectedHandler != null) {
                        disconnectedHandler.handle(null);
                    }

                    connectTimer.stop();
                }
            });

            if (connectedHandler != null) {
                connectedHandler.handle(Future.succeededFuture());
            }
        }

        protected void removeMessageFromInflight(String messageId) {
            inflightMessages.dec();
            final Timer.Context context = inflightTimers.remove(messageId);
            if (context != null) {
                context.stop();
            }
            final Long timeoutBucketId = messageTimeoutBucketMap.remove(messageId);
            if (timeoutBucketId != null) {
                final ConcurrentHashMap<String, Request> timeoutBucket = timeoutBuckets.get(timeoutBucketId);
                if (timeoutBucket != null) {
                    timeoutBucket.remove(messageId);
                } else {
                    responsesAlreadyTimedout.mark();
                }
            }
        }

        public void disconnectedHandler(Handler<Void> disconnectedHandler) {

            this.disconnectedHandler = disconnectedHandler;
        }

        public void connectedHandler(Handler<AsyncResult<Void>> connectedHandler) {

            this.connectedHandler = connectedHandler;
        }

        public void addSocketToPumps(final NetSocket sock, final Connection connection, final Timer.Context connectTimer) {
            final ReadStream<Buffer> readStream = wrapReadStream(sock, connection, receivePump);
            multiReadPump.add(readStream);
            sock.closeHandler(new Handler<Void>()

                              {
                                  @Override
                                  public void handle(Void event) {
                                      multiReadPump.remove(readStream);
                                      sock.drainHandler(null);
                                      connection.setCloseTimestamp(System.currentTimeMillis());
//                                      connectionProducer.onData(connection);
                                      connectTimer.stop();
                                  }
                              }
            );
        }
    }


}
