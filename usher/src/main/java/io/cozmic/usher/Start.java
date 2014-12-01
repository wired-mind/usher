package io.cozmic.usher;


import io.cozmic.pulsar.provider.PulsarProviderServer;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.file.impl.PathAdjuster;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by chuck on 10/23/14.
 */
public class Start extends Verticle {

    public static final String TIMEOUT_REPLAY_HANDLER = "TIMEOUT_REPLAY_HANDLER";
    private JsonObject proxyConfig;

    public void start(final Future<Void> startedResult) {

        proxyConfig = container.config().getObject("proxyConfig");

        container.logger().info("Config: " + container.config().toString());
        final Handler<AsyncResult<String>> doneHandler = prerequisitesReadyHandler(startedResult, 6);

        final JsonObject defaultConfigFixedTwo = new JsonObject().putString("type", "fixed").putNumber("length", 2);
        JsonObject responseParsingRules = container.config().getObject("responseParsingRules", defaultConfigFixedTwo);

        final JsonObject pulsarConfig = new JsonObject().putObject("responseParsingConfig", responseParsingRules);
        container.deployModule("io.cozmic~pulsar-provider~1.0.0-final", pulsarConfig, doneHandler);
        container.deployModule("io.cozmic~pulsar-server~1.0.0-final", doneHandler);

        final int instanceCountForRocksVerticles = 1; //Only one thread can access the rocksDB
        container.deployWorkerVerticle(PersistenceVerticle.class.getName(), container.config().getObject("persistence", new JsonObject()), instanceCountForRocksVerticles, true, doneHandler);
        container.deployVerticle(EchoChamber.class.getName(), doneHandler);
        container.deployVerticle(CommandVerticle.class.getName(), doneHandler);
        final JsonObject shellConfig = container.config().getObject("shellConfig");
        if (shellConfig != null) {
            final String commandDir = PathAdjuster.adjust((VertxInternal) vertx, "./commands");
            container.logger().info("Setting custom crash command directory to " + commandDir);
            shellConfig.putString("cmd", commandDir);
        }
        container.deployModule("org.crashub~vertx.shell~2.1.0", shellConfig, doneHandler);


        /**
         * When pulsar starts, register the timeout replay handler
         */
        vertx.eventBus().registerLocalHandler(PulsarProviderServer.PULSAR_PROVIDER_SERVER_STARTED, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> msg) {
                final JsonObject pulsarInfo = msg.body();

                /**
                 * Each node handles timeout replay with pulsar independently
                 */
                vertx.eventBus().registerHandler(TIMEOUT_REPLAY_HANDLER, new Handler<Message<String>>() {
                    @Override
                    public void handle(final Message<String> msg) {

                        final String reply = msg.body();
                        //Run query to find the start and end range for the current timeout log
                        //This endpoint should be a local handler so it is specific to this node
                        vertx.eventBus().send(PersistenceVerticle.TIMEOUT_LOG_RANGE_QUERY_ADDRESS, new JsonObject(), new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> response) {
                                final JsonObject pulsarCommand = response.body();
                                if (!pulsarCommand.getString("status").equals("ok")) {
                                    vertx.eventBus().send(reply, pulsarCommand);
                                    return;
                                }

                                pulsarCommand
                                        .putNumber("concurrency", 10)
                                        .putObject("target", new JsonObject()
                                                .putString("host", proxyConfig.getString("serviceHost"))
                                                .putNumber("port", proxyConfig.getNumber("servicePort")))
                                        .putObject("pulsarProvider", new JsonObject()
                                                .putString("host", pulsarInfo.getString("host"))
                                                .putNumber("port", pulsarInfo.getNumber("port")))

                                ;

                                vertx.eventBus().send("REQUEST_RANGE", pulsarCommand, new Handler<Message<JsonObject>>() {
                                    @Override
                                    public void handle(Message<JsonObject> event) {
                                        final String status = event.body().getString("status");
                                        if (status.equals("ok")) {
                                            vertx.eventBus().send(PersistenceVerticle.TIMEOUT_LOG_DELETE_ADDRESS, pulsarCommand, new Handler<Message<JsonObject>>() {
                                                @Override
                                                public void handle(Message<JsonObject> event) {
                                                    vertx.eventBus().send(reply, event.body());
                                                }
                                            });
                                        } else {
                                            vertx.eventBus().send(reply, event.body());
                                        }
                                    }
                                });

                            }
                        });
                    }
                });
            }
        });


    }



    protected Handler<AsyncResult<String>> prerequisitesReadyHandler(final Future<Void> startedResult, final int numberOfPrerequisites) {
        final AtomicInteger prereqsLaunched = new AtomicInteger();
        return new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> asyncResult) {
                if (asyncResult.failed()) {
                    final Throwable cause = asyncResult.cause();
                    container.logger().error(cause.getMessage(), cause);
                    startedResult.setFailure(cause);
                    stop();
                    return;
                }

                final int launchedSoFar = prereqsLaunched.incrementAndGet();
                container.logger().info("Deployed prereq");
                if (launchedSoFar == numberOfPrerequisites) {
                    final int proxyInstances = Runtime.getRuntime().availableProcessors() / 2;

                    container.deployVerticle(Proxy.class.getName(), proxyConfig, proxyInstances, new Handler<AsyncResult<String>>() {
                        @Override
                        public void handle(AsyncResult<String> asyncResult) {
                            if (asyncResult.failed()) {
                                container.logger().error(asyncResult.cause());
                                startedResult.setFailure(asyncResult.cause());
                                stop();
                                return;
                            }

                            container.logger().info("Proxy deployed");
                            startedResult.setResult(null);
                        }
                    });
                }
            }
        };
    }


}


