package io.cozmic.usher.core;

import com.codahale.metrics.*;
import io.cozmic.usher.peristence.MessageEventProducer;
import io.cozmic.usher.peristence.JournalingWriteStream;
import io.cozmic.usherprotocols.core.CozmicPump;
import io.cozmic.usherprotocols.core.Message;
import io.cozmic.usherprotocols.core.MessageReadStream;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;
import org.vertx.java.platform.Container;

import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by chuck on 10/24/14.
 */
public abstract class ProxyTunnel {
    public static final MetricRegistry metrics = new MetricRegistry();
    public static final int ONE_SECOND_IN_MILLIS = 1000;
    public static final int STARTING_BUCKET_ID = 1;
    private final Timer responses = metrics.timer(name(ProxyTunnel.class, "responses"));
    private final Timer clientConnects = metrics.timer(name(ProxyTunnel.class, "client-connects"));
    private final Timer serviceConnects = metrics.timer(name(ProxyTunnel.class, "service-connects"));
    private final Counter inflightMessages = metrics.counter(name(ProxyTunnel.class, "inflight-messages"));
    private final Meter timeouts = metrics.meter("timeouts");
    private final Meter clientErrors = metrics.meter("client-errors");
    private final Meter serviceErrors = metrics.meter("service-errors");
    private final ConcurrentHashMap<String, Timer.Context> inflightTimers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Double, ConcurrentHashMap<String, Message>> timeoutBuckets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double> messageTimeoutBucketMap = new ConcurrentHashMap<>();

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 2500;
    public static final int DEFAULT_TIMEOUT = 30000;
    public static final int ALWAYS_ATTEMPT_RECONNECT = -1;
    private final Vertx vertx;
    private final MessageEventProducer journalProducer;
    private final MessageEventProducer timeoutLogProducer;
    private NetServer netServer;
    private final NetClient netClient;
    private final Logger log;
    private final String proxyHost;
    private final Integer proxyPort;
    private final Integer proxyTimeout;
    private ServiceGateway serviceGateway;

    static {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(3, TimeUnit.SECONDS);
    }

    public ProxyTunnel(Container container, final Vertx vertx, final MessageEventProducer journalProducer, final MessageEventProducer timeoutLogProducer) {
        this.vertx = vertx;
        this.journalProducer = journalProducer;
        this.timeoutLogProducer = timeoutLogProducer;

        proxyHost = container.config().getString("host", DEFAULT_HOST);
        proxyPort = container.config().getInteger("port", DEFAULT_PORT);
        proxyTimeout = container.config().getInteger("timeout", DEFAULT_TIMEOUT);
        log = container.logger();
        log.info("[ProxyTunnel] Initializing");
        netClient = vertx.createNetClient();
        netClient.setConnectTimeout(ONE_SECOND_IN_MILLIS * 10);
        netClient.setReconnectAttempts(ALWAYS_ATTEMPT_RECONNECT);
        netClient.setReconnectInterval(500);

        serviceGateway = new ServiceGateway();


        netServer = vertx.createNetServer();
        netServer.setAcceptBacklog(10000);


        netServer.connectHandler(new Handler<NetSocket>() {
            @Override
            public void handle(final NetSocket sock) {
                final Timer.Context connectTimer = clientConnects.time();
                sock.exceptionHandler(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable event) {
                        log.error("[ProxyTunnel] Socket error", event);
                        connectTimer.stop();
                        clientErrors.mark();
                    }
                });

                serviceGateway.addSocketToPumps(sock, connectTimer);

            }
        });





    }

    protected double currentTimeoutBucketId() {
        return STARTING_BUCKET_ID + (nearestSecondInMillis() % proxyTimeout);
    }

    private double nearestSecondInMillis() {
        return Math.ceil(System.currentTimeMillis() / ONE_SECOND_IN_MILLIS) * ONE_SECOND_IN_MILLIS;
    }

    public void listen(final Handler<AsyncResult<Void>> handler) {
        netServer.listen(proxyPort, proxyHost, new Handler<AsyncResult<NetServer>>() {
            @Override
            public void handle(AsyncResult<NetServer> event) {
                if (!event.succeeded()) {
                    log.error(event.cause());
                    handler.handle(new DefaultFutureResult<Void>(event.cause()));
                    return;
                }

                log.info("[ProxyTunnel] Ready to accept connections");
                handler.handle(new DefaultFutureResult<>((Void) null));
            }
        });
    }

    public void connect(final Integer servicePort, final String serviceHost, final Handler<AsyncResult<Void>> readyHandler) {

        serviceGateway.disconnectedHandler(new Handler<Void>() {
            @Override
            public void handle(Void event) {
                log.info("[ServiceGateway] disconnected. Closing [ProxyTunnel] listener.");
                netServer.close(new Handler<AsyncResult<Void>>() {
                    @Override
                    public void handle(AsyncResult<Void> event) {
                        if (!event.succeeded()) {
                            log.error(event.cause());
                            return;
                        }

                        log.info("[ProxyTunnel] closed after [ServiceGateway] disconnect. Attempting to reestablish connection.");
                        netClient.connect(servicePort, serviceHost, serviceGateway);
                    }
                });



            }
        });

        serviceGateway.connectedHandler(readyHandler);
        netClient.connect(servicePort, serviceHost, serviceGateway);
    }



    protected abstract MessageReadStream<?> wrapWithMessageReader(NetSocket serviceSocket);

    protected abstract ReadStream<?> wrapReadStream(NetSocket sock, CozmicPump receivePump);

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

            for (double bucketId = STARTING_BUCKET_ID; bucketId < proxyTimeout; bucketId+=ONE_SECOND_IN_MILLIS) {
                final ConcurrentHashMap<String, Message> timeoutBucket = new ConcurrentHashMap<>();
                timeoutBuckets.put(bucketId, timeoutBucket);
                //Start a timer for each bucket that lets us delay the start of the periodic timer set to the actual proxyTimeout
                vertx.setTimer((long) bucketId, new Handler<Long>() {
                    @Override
                    public void handle(Long event) {
//                        vertx.setPeriodic(proxyTimeout, new Handler<Long>() {
//                            @Override
//                            public void handle(Long event) {
//                                for (String messageId : timeoutBucket.keySet()) {
//                                    final Message message = timeoutBucket.get(messageId);
//                                    receivePump.timeoutMessage(messageId);
//                                    timeoutLogProducer.onData(message);
//                                    timeouts.mark();
//                                }
//                            }
//                        });
                    }
                });
            }

        }

        public int getConnectionCount() {
            return multiReadPump.getConnectionCount();
        }

        @Override
        public void handle(AsyncResult<NetSocket> asyncResult) {
            if (asyncResult.failed()) {
                log.error(asyncResult.cause());
                if (connectedHandler != null) {
                    connectedHandler.handle(new DefaultFutureResult<Void>(asyncResult.cause()));
                }
                return;
            }
            final Timer.Context connectTimer = serviceConnects.time();

            final NetSocket serviceSocket = asyncResult.result();

            final MessageReadStream<?> messageReadStream = wrapWithMessageReader(serviceSocket);
            final WriteStream<?> journalingServiceSocket = new JournalingWriteStream(serviceSocket, journalProducer);
            multiReadPump.setWriteStream(journalingServiceSocket);
            receivePump.setMessageReadStream(messageReadStream);
            receivePump.inflightHandler(new Handler<Message>() {
                @Override
                public void handle(final Message message) {
                    inflightMessages.inc();
                    inflightTimers.put(message.getMessageId(), responses.time());
                    final double currentTimeoutBucketId = currentTimeoutBucketId();
                    final ConcurrentHashMap<String, Message> timeoutBucket = timeoutBuckets.get(currentTimeoutBucketId);
//                    timeoutBucket.put(message.getMessageId(), message);
//                    messageTimeoutBucketMap.put(message.getMessageId(), currentTimeoutBucketId);
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
                    log.error("[ServiceGateway] socket error", event);
                    serviceErrors.mark();
                }
            });
            serviceSocket.closeHandler(new Handler<Void>() {
                @Override
                public void handle(Void event) {
                    log.info("[ServiceGateway] socket closing");
                    multiReadPump.setWriteStream(null);
                    receivePump.setMessageReadStream(null);
                    if (disconnectedHandler != null) {
                        disconnectedHandler.handle(null);
                    }

                    connectTimer.stop();
                }
            });

            if (connectedHandler != null) {
                connectedHandler.handle(new DefaultFutureResult<Void>((Void) null));
            }
        }

        protected void removeMessageFromInflight(String messageId) {
            inflightMessages.dec();
            final Timer.Context context = inflightTimers.remove(messageId);
            if (context != null) {
                context.stop();
            }
            final Double timeoutBucketId = messageTimeoutBucketMap.remove(messageId);
            if (timeoutBucketId != null) {
                final ConcurrentHashMap<String, Message> timeoutBucket = timeoutBuckets.get(timeoutBucketId);
                timeoutBucket.remove(messageId);
            }
        }

        public void disconnectedHandler(Handler<Void> disconnectedHandler) {

            this.disconnectedHandler = disconnectedHandler;
        }

        public void connectedHandler(Handler<AsyncResult<Void>> connectedHandler) {

            this.connectedHandler = connectedHandler;
        }

        public void addSocketToPumps(final NetSocket sock, final Timer.Context connectTimer) {
            final ReadStream<?> readStream = wrapReadStream(sock, receivePump);
            multiReadPump.add(readStream);
            sock.closeHandler(new Handler<Void>()

                              {
                                  @Override
                                  public void handle(Void event) {
                                      multiReadPump.remove(readStream);
                                      sock.drainHandler(null);
                                      connectTimer.stop();
                                  }
                              }
            );
        }
    }
}
