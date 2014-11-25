package io.cozmic.usher;

import io.cozmic.usher.peristence.JournalRepository;
import io.cozmic.usher.peristence.TimeSeqRepository;
import org.rocksdb.RocksDBException;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * Created by chuck on 10/31/14.
 */
public class PersistenceVerticle extends Verticle {
    public static final String TIMEOUT_LOG_ADDRESS = "timeout-logger";
    public static final String JOURNAL_ADDRESS = "journal";
    public static final String TIMEOUT_LOG_COUNT_ADDRESS = "timeout-logger-count";
    public static final String TIMEOUT_LOG_LIST_ADDRESS = "timeout-logger-list";
    public static final String TIMEOUT_LOG_REPLAY_ADDRESS = "timeout-logger-replay";
    public static final String CONNECTION_LOG_ADDRESS = "connection-logger";
    private TimeSeqRepository timeoutLogRepository;
    private TimeSeqRepository connectionLogRepository;
    private JournalRepository journalRepository;


    public void stop() {
        journalRepository.dispose();
        timeoutLogRepository.dispose();
    }

    public void start(final Future<Void> startedResult) {
        try {
            setupConnectionLog(container.config().getObject("connectionLogConfig", new JsonObject()));
            setupJournal(container.config().getObject("journalerConfig", new JsonObject()));
            setupTimeoutLog(container.config().getObject("timeoutLogConfig", new JsonObject()));
        } catch (RocksDBException e) {
            container.logger().error(e);
            startedResult.setFailure(e);
            return;
        }

        startedResult.setResult(null);
    }

    private void setupConnectionLog(JsonObject config) throws RocksDBException {
        final String dbPath = config.getString("dbPath", "connections");
        connectionLogRepository = new TimeSeqRepository(dbPath);

        vertx.eventBus().registerLocalHandler(CONNECTION_LOG_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message<Buffer>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<Buffer> msg) {
                final Buffer buffer = msg.body();
                try {
                    connectionLogRepository.insert(buffer.getBytes());
                } catch (RocksDBException e) {
                    container.logger().error(e);
                }
            }
        });
    }

    private void setupTimeoutLog(JsonObject config) throws RocksDBException {
        final String dbPath = config.getString("dbPath", "timeouts");
        timeoutLogRepository = new TimeSeqRepository(dbPath);

        vertx.eventBus().registerLocalHandler(TIMEOUT_LOG_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message<Buffer>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<Buffer> msg) {

                final Buffer envelope = msg.body();
                try {
                    timeoutLogRepository.insert(envelope.getBytes());
                } catch (RocksDBException e) {
                    container.logger().error(e);
                }
            }
        });

        vertx.eventBus().registerHandler(TIMEOUT_LOG_COUNT_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message<String>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<String> msg) {
                container.logger().info("Received at " + TIMEOUT_LOG_COUNT_ADDRESS + " value: " + msg.body());
                long count;
                try {
                    count = timeoutLogRepository.count();
                    vertx.eventBus().send(msg.body(), count);
                } catch (RocksDBException e) {
                    container.logger().error(e);
                }
            }
        });

        vertx.eventBus().registerHandler(TIMEOUT_LOG_LIST_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message<String>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<String> msg) {
                container.logger().info("Received at " + TIMEOUT_LOG_LIST_ADDRESS + " value: " + msg.body());

//                try {
//                    Map<String, Reply> messages = timeoutLogRepository.a();
//                    for (String messageId : messages.keySet()) {
//                        final Reply reply = messages.get(messageId);
//                        vertx.eventBus().send(msg.body(), String.format("MessageId: %s Message: %s\n\n", messageId, reply.getBody().toString()));
//                    }
//
//                } catch (RocksDBException e) {
//                    container.logger().error(e);
//                }
            }
        });
    }

    private void setupJournal(JsonObject config) throws RocksDBException {
        final String dbPath = config.getString("dbPath", "journal");
        journalRepository = new JournalRepository(dbPath);

        vertx.eventBus().registerLocalHandler(JOURNAL_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message<Buffer>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<Buffer> msg) {
                final Buffer envelope = msg.body();

                try {
                    journalRepository.insert(envelope.getBytes());
                } catch (Exception e) {
                    container.logger().error(e);
                }
            }
        });
    }
}