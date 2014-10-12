package io.cozmic.usher;

import com.lmax.disruptor.dsl.Disruptor;
import io.cozmic.usher.core.*;
import io.cozmic.usher.journal.*;
import io.cozmic.usher.protocols.ConfigurablePacketSocket;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.shareddata.ConcurrentSharedMap;
import org.vertx.java.core.streams.ReadStream;
import org.vertx.java.core.streams.WriteStream;
import org.vertx.java.platform.Verticle;


import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by chuck on 9/8/14.
 */
public abstract class Proxy extends Verticle {


    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 2500;
    public static final int DEFAULT_TIMEOUT = 30000;

    private Disruptor<JournalEvent> disruptor;


    private NetServer netServer;


    protected abstract ReceiveProcessor createReceiveProcessor();

    // sendHandler is invoked right before writing the message to the writeStream
    protected abstract MessageProcessingPump createSendPump(ReadStream<?> readStream, WriteStream<?> writeStream, MessageTranslator translator);

    public void start(final Future<Void> startedResult) {


        final ConcurrentMap<String, Message> inflight = vertx.sharedData().getMap("inflight");
        final Logger log = container.logger();
        final ConcurrentSharedMap<String, Counter> counters = vertx.sharedData().getMap("counters");

        final String proxyHost = container.config().getString("host", DEFAULT_HOST);
        final Integer proxyPort = container.config().getInteger("port", DEFAULT_PORT);
        final Integer proxyTimeout = container.config().getInteger("timeout", DEFAULT_TIMEOUT);
        final Integer servicePort = container.config().getInteger("service_port", 9191);
        final String serviceHost = container.config().getString("service_host", "localhost");


        disruptor = createDisruptor();
        // Connect the handler
        disruptor.handleEventsWith(new JournalEventHandler(vertx));

        // Start the Disruptor, starts all threads running
        disruptor.start();


        final JournalEventProducer producer = new JournalEventProducer(disruptor.getRingBuffer());


        netServer = vertx.createNetServer();
        final NetClient netClient = vertx.createNetClient();

        netClient.setReconnectAttempts(1000);
        netClient.setReconnectInterval(500);

        netClient.connect(servicePort, serviceHost, new Handler<AsyncResult<NetSocket>>() {
            @Override
            public void handle(AsyncResult<NetSocket> asyncResult) {

                if (asyncResult.failed()) {
                    log.error(asyncResult.cause());
                    startedResult.setFailure(asyncResult.cause());
                    return;
                }

                final NetSocket serviceSocket = asyncResult.result();


                final MultiReadPump pump = MultiReadPump.createPump(serviceSocket);
                serviceSocket.exceptionHandler(new Handler<Throwable>() {
                    @Override
                    public void handle(Throwable event) {
                        log.error("socket error on proxy service socket", event);
                    }
                });
                final CozmicSocket cozmicSocket = new CozmicSocket(serviceSocket, counters.get("receive"));
                final CozmicPump receivePump = CozmicPump.createPump(cozmicSocket).start();

//                final WriteStream<?> journalingServiceSocket = new JournalingWriteStream(serviceSocket, producer);
//                log.info("We have connected! Socket is " + journalingServiceSocket);


                netServer.setAcceptBacklog(10000);
//                netServer.setReceiveBufferSize(34*512);
//                netServer.setSendBufferSize(2*512);

                netServer.connectHandler(new Handler<NetSocket>() {
                    @Override
                    public void handle(final NetSocket sock) {
                        sock.exceptionHandler(new Handler<Throwable>() {
                            @Override
                            public void handle(Throwable event) {
                                log.error("Socket error on proxy client socket", event);
                            }
                        });

                        final ConfigurablePacketSocket configurablePacketSocket = new ConfigurablePacketSocket(sock, counters.get("purged_proxy"), counters.get("received_bytes"));


//                                pump.add(configurablePacketSocket);

//                                Pump.createPump(serviceSocket, sock).start();


                        pump.add(configurablePacketSocket.translate(new StreamProcessor() {
                            @Override
                            public void process(Buffer data, Handler<AsyncResult<Buffer>> resultHandler) {
                                final String messageId = UUID.randomUUID().toString();
                                int messageLength = 4 + 4 + messageId.length() + data.length();
                                final Buffer envelope = new Buffer(messageLength);
                                envelope.appendInt(messageLength);
                                envelope.appendInt(messageId.length());
                                envelope.appendString(messageId);
                                envelope.appendBuffer(data);


                                vertx.eventBus().send("stats:proxyReceive", 1);
                                receivePump.add(messageId, sock);
                                resultHandler.handle(new DefaultFutureResult<Buffer>(envelope));
                            }
                        }));


                        sock.closeHandler(new Handler<Void>()

                                          {
                                              @Override
                                              public void handle(Void event) {
//                                                          log.info("Proxy: client socket closing");
                                                  pump.remove(configurablePacketSocket);
                                                  sock.drainHandler(null);
                                              }
                                          }

                        );

                    }
                }).listen(proxyPort, proxyHost, new Handler<AsyncResult<NetServer>>() {
                    @Override
                    public void handle(AsyncResult<NetServer> event) {
                        if (!event.succeeded()) {
                            log.error(event.cause());
                            startedResult.setFailure(event.cause());
                            return;
                        }

                        log.info("Proxy ready");
                        startedResult.setResult(null);
                    }
                });


                serviceSocket.closeHandler(new Handler<Void>() {
                    @Override
                    public void handle(Void event) {
                        log.info("Service (receive) socket closing");
                    }
                });


            }
        });


    }

    protected Disruptor<JournalEvent> createDisruptor() {
        // Executor that will be used to construct new threads for consumers
        Executor executor = Executors.newCachedThreadPool();

        // The factory for the event
        JournalEventFactory factory = new JournalEventFactory();

        // Construct the Disruptor
        return new Disruptor<>(factory, 1024, executor);
    }

    public void stop() {
        if (netServer != null) netServer.close(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                disruptor.shutdown();
            }
        });
    }

}
