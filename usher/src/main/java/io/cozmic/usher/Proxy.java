package io.cozmic.usher;


import io.cozmic.usher.old.ProxyTunnel;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;


/**
 * Created by chuck on 9/8/14.
 */
public class Proxy extends AbstractVerticle {

    Logger logger = LoggerFactory.getLogger(Proxy.class.getName());

    public void start(final Future<Void> startFuture) {
        final Integer servicePort = config().getInteger("servicePort", EchoChamber.ECHO_SERVICE_PORT);
        final String serviceHost = config().getString("serviceHost", EchoChamber.ECHO_SERVICE_HOST);
        final JsonArray services = config().getJsonArray("services");




    }



    protected void createAndStartProxyTunnel(final Integer servicePort, final String serviceHost, final Future<Void> startedResult) {
        createProxyTunnel(new Handler<AsyncResult<ProxyTunnel>>() {
            @Override
            public void handle(AsyncResult<ProxyTunnel> asyncResult) {
                if (asyncResult.failed()) {
                    logger.error(asyncResult.cause());
                    startedResult.fail(asyncResult.cause());
                    return;
                }

                final ProxyTunnel proxyTunnel = asyncResult.result();
                proxyTunnel.connect(servicePort, serviceHost, new AsyncResultHandler<Void>() {
                    @Override
                    public void handle(AsyncResult<Void> asyncResult) {
                        if (asyncResult.failed()) {
                            logger.error(asyncResult.cause());
                            startedResult.fail(asyncResult.cause());
                            return;
                        }

                        logger.info(String.format("[ProxyTunnel] Connected to %s:%s", serviceHost, servicePort));
                        proxyTunnel.listen(new AsyncResultHandler<Void>() {
                            @Override
                            public void handle(AsyncResult<Void> asyncResult) {
                                if (asyncResult.failed()) {
                                    logger.error(asyncResult.cause());
                                    startedResult.fail(asyncResult.cause());
                                    return;
                                }

                                logger.info(String.format("[ProxyTunnel] Started. Listening on %s:%s", proxyTunnel.getProxyHost(), proxyTunnel.getProxyPort()));
                                startedResult.complete(null);
                            }
                        });
                    }
                });

            }
        });
    }

    protected void createProxyTunnel(Handler<AsyncResult<ProxyTunnel>> handler) {

        final String proxyTunnelType = config().getString("proxy_tunnel_type", "io.cozmic.usher.old.NetProxyTunnel");
        try {
            final Class<?> proxyTunnelClass = Class.forName(proxyTunnelType);
            final Constructor<?> constructor = proxyTunnelClass.getConstructor(Context.class, Vertx.class);
            final ProxyTunnel proxyTunnel = (ProxyTunnel) constructor.newInstance(context, vertx, null, null, null);
            handler.handle(Future.succeededFuture(proxyTunnel));

        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException  e) {
            logger.error(e);
            handler.handle(Future.failedFuture(e));
        }
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
