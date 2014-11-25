package io.cozmic.usher;

import com.hazelcast.client.config.ClientConfig;
import com.lmax.disruptor.dsl.Disruptor;
import io.cozmic.usher.core.ClusterListener;
import io.cozmic.usher.core.HazelcastClusterListener;
import io.cozmic.usher.core.Listener;
import io.cozmic.usher.core.ProxyTunnel;
import io.cozmic.usher.peristence.*;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.platform.Container;
import org.vertx.java.platform.Verticle;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Created by chuck on 9/8/14.
 */
public class Proxy extends Verticle {


    private Disruptor<ConnectionEvent> connectionDisruptor;
    private Disruptor<RequestEvent> journalDisruptor;
    private Disruptor<RequestEvent> timeoutLogDisruptor;

    private ConnectionEventProducer connectionProducer;
    private RequestEventProducer journalProducer;
    private RequestEventProducer timeoutLogProducer;


    public void start(final Future<Void> startedResult) {
        final Integer servicePort = container.config().getInteger("servicePort", EchoChamber.ECHO_SERVICE_PORT);
        final String serviceHost = container.config().getString("serviceHost", EchoChamber.ECHO_SERVICE_HOST);
        final String serviceClusterHost = container.config().getString("service_cluster_host");


        connectionDisruptor = createConnectionDisruptor();
        journalDisruptor = createRequestDisruptor();
        timeoutLogDisruptor = createRequestDisruptor();

        // Connect the handler
        connectionDisruptor.handleEventsWith(new ConnectionEventHandler(vertx));
        journalDisruptor.handleEventsWith(new JournalEventHandler(vertx));
        timeoutLogDisruptor.handleEventsWith(new TimeoutRequestEventHandler(vertx));

        // Start the Disruptor, starts all threads running
        connectionDisruptor.start();
        journalDisruptor.start();
        timeoutLogDisruptor.start();

        connectionProducer = new ConnectionEventProducer(connectionDisruptor.getRingBuffer());
        journalProducer = new RequestEventProducer(journalDisruptor.getRingBuffer());
        timeoutLogProducer = new RequestEventProducer(timeoutLogDisruptor.getRingBuffer());

        Map<String, InetSocketAddress> members = new ConcurrentHashMap<>();



        if (serviceHost != null) {
            members.put("DEFAULT", new InetSocketAddress(serviceHost, servicePort));
        }
        if (serviceClusterHost != null) {
            //TODO: Fix this
            final ClientConfig clientConfig = new ClientConfig();

            ClusterListener clusterListener = new HazelcastClusterListener(clientConfig);
            members.putAll(clusterListener.getMembers());

            clusterListener.listener(new Listener() {

                @Override
                public void added(String uuid, String serviceHost, int servicePort) {

                }

                @Override
                public void removed(String uuid) {

                }
            }).start();

        }
        final int serviceMemberCount = members.size();
        if (serviceMemberCount <= 0) {
            throw new RuntimeException("No service configured. Cannot start Usher Proxy.");
        }
        final Future<Void> futureResult = new CountDownFutureResult<>(serviceMemberCount);
        for (String uuid : members.keySet()) {
            final InetSocketAddress socketAddress = members.get(uuid);
            createAndStartProxyTunnel(socketAddress.getPort(), socketAddress.getHostName(), futureResult);
        }

        futureResult.setHandler(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> asyncResult) {
                if (asyncResult.failed()) {
                    container.logger().error(asyncResult.cause());
                    startedResult.setFailure(asyncResult.cause());
                    return;
                }

                container.logger().info(String.format("[Proxy] Started using %s Proxy Tunnels", serviceMemberCount));
                startedResult.setResult(null);
            }
        });

    }

    protected void createAndStartProxyTunnel(final Integer servicePort, final String serviceHost, final Future<Void> startedResult) {
        createProxyTunnel(new Handler<AsyncResult<ProxyTunnel>>() {
            @Override
            public void handle(AsyncResult<ProxyTunnel> asyncResult) {
                if (asyncResult.failed()) {
                    container.logger().error(asyncResult.cause());
                    startedResult.setFailure(asyncResult.cause());
                    return;
                }

                final ProxyTunnel proxyTunnel = asyncResult.result();
                proxyTunnel.connect(servicePort, serviceHost, new Handler<AsyncResult<Void>>() {
                    @Override
                    public void handle(AsyncResult<Void> asyncResult) {
                        if (asyncResult.failed()) {
                            container.logger().error(asyncResult.cause());
                            startedResult.setFailure(asyncResult.cause());
                            return;
                        }

                        container.logger().info(String.format("[ProxyTunnel] Connected to %s:%s", serviceHost, servicePort));
                        proxyTunnel.listen(new Handler<AsyncResult<Void>>() {
                            @Override
                            public void handle(AsyncResult<Void> asyncResult) {
                                if (asyncResult.failed()) {
                                    container.logger().error(asyncResult.cause());
                                    startedResult.setFailure(asyncResult.cause());
                                    return;
                                }

                                container.logger().info(String.format("[ProxyTunnel] Started. Listening on %s:%s", proxyTunnel.getProxyHost(), proxyTunnel.getProxyPort()));
                                startedResult.setResult(null);
                            }
                        });
                    }
                });

            }
        });
    }

    protected void createProxyTunnel(Handler<AsyncResult<ProxyTunnel>> handler) {

        final String proxyTunnelType = container.config().getString("proxy_tunnel_type", "io.cozmic.usher.core.NetProxyTunnel");
        try {
            final Class<?> proxyTunnelClass = Class.forName(proxyTunnelType);
            final Constructor<?> constructor = proxyTunnelClass.getConstructor(Container.class, Vertx.class, ConnectionEventProducer.class, RequestEventProducer.class, RequestEventProducer.class);
            final ProxyTunnel proxyTunnel = (ProxyTunnel) constructor.newInstance(container, vertx, connectionProducer, journalProducer, timeoutLogProducer);
            handler.handle(new DefaultFutureResult<>(proxyTunnel));

        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException  e) {
            container.logger().error(e);
            handler.handle(new DefaultFutureResult<ProxyTunnel>(e));
        }
    }

    private Disruptor<ConnectionEvent> createConnectionDisruptor() {
        // Executor that will be used to construct new threads for consumers
        Executor executor = Executors.newCachedThreadPool();

        // The factory for the event
        ConnectionEventFactory factory = new ConnectionEventFactory();

        // Construct the Disruptor
        return new Disruptor<>(factory, 1024, executor);
    }




    protected Disruptor<RequestEvent> createRequestDisruptor() {
        // Executor that will be used to construct new threads for consumers
        Executor executor = Executors.newCachedThreadPool();

        // The factory for the event
        RequestEventFactory factory = new RequestEventFactory();

        // Construct the Disruptor
        return new Disruptor<>(factory, 1024, executor);
    }

    public void stop() {
//        Todo: Need a graceful shutdown routine
//        if (netServer != null) netServer.close(new Handler<AsyncResult<Void>>() {
//            @Override
//            public void handle(AsyncResult<Void> event) {
//                journalDisruptor.shutdown();
//            }
//        });
    }


}
