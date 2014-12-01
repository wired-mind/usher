package io.cozmic.usher;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetServer;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;
import org.vertx.java.platform.Verticle;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created by chuck on 11/7/14.
 */
public class CommandVerticle extends Verticle {

    public void start(final Future<Void> startedResult) {



        vertx.createNetServer().connectHandler(new Handler<NetSocket>() {
            @Override
            public void handle(final NetSocket socket) {
                socket.dataHandler(RecordParser.newDelimited(System.lineSeparator(), new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer data) {
                        final String command = data.toString().trim();
                        container.logger().info("Command received: " + command);
                        final String[] commands = command.split(" ");
                        switch (commands[0]) {
                            case "timeouts":
                                handleTimeouts(commands[1], new Handler<String>() {
                                    @Override
                                    public void handle(String result) {
                                        socket.write(result);
                                    }
                                });
                                break;
                        }
                    }
                }));
            }
        }).listen(2001, new Handler<AsyncResult<NetServer>>() {
            @Override
            public void handle(AsyncResult<NetServer> asyncResult) {
                if (asyncResult.failed()) {
                    final Throwable cause = asyncResult.cause();
                    container.logger().error(cause.getMessage(), cause);
                    startedResult.setFailure(cause);
                    stop();
                    return;
                }
                startedResult.setResult(null);
            }
        });
    }

    private void handleTimeouts(String subCommand, final Handler<String> resultHandler) {
        final String reply = UUID.randomUUID().toString();
        final AtomicLong runningTotal = new AtomicLong();
        switch (subCommand) {
            case "count":
                final AtomicInteger nodeCount = new AtomicInteger();


                vertx.eventBus().registerHandler(reply, new Handler<Message<Long>>() {
                    @Override
                    public void handle(Message<Long> msg) {
                        final Long count = msg.body();
                        final long total = runningTotal.addAndGet(count);
                        final int nodeNumber = nodeCount.incrementAndGet();
                        resultHandler.handle(String.format("Node %s Count: %d Total: %d\n\n", nodeNumber, count, total));
                    }
                });

                container.logger().info("Sending to:" + PersistenceVerticle.TIMEOUT_LOG_COUNT_ADDRESS + " value: " + reply);
                vertx.eventBus().publish(PersistenceVerticle.TIMEOUT_LOG_COUNT_ADDRESS, reply);
                break;

            case "list":

                vertx.eventBus().publish(PersistenceVerticle.TIMEOUT_LOG_LIST_ADDRESS, reply);

                vertx.eventBus().registerHandler(reply, new Handler<Message<String>>() {
                    @Override
                    public void handle(Message<String> msg) {
                        final long total = runningTotal.incrementAndGet();
                        resultHandler.handle(String.format("Running total: %d . %s\n", total, msg.body()));
                    }
                });
                break;

            case "replay": {
                vertx.eventBus().publish(Start.TIMEOUT_REPLAY_HANDLER, reply);
                vertx.eventBus().registerHandler(reply, new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> msg) {
                        resultHandler.handle(String.format("%s - %s\n", msg.body().getString("status"), msg.body().getString("message")));
                    }
                });

                break;
            }
        }
    }


}
