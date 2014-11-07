package io.cozmic.usher;

import io.cozmic.usher.peristence.JournalRepository;
import io.cozmic.usher.peristence.TimeoutLogRepository;
import org.rocksdb.RocksDBException;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import io.cozmic.usherprotocols.core.Message;

import java.nio.ByteBuffer;
import java.util.Calendar;

/**
 * Created by chuck on 10/31/14.
 */
public class PersistenceVerticle extends Verticle {
    public static final String TIMEOUT_LOG_ADDRESS = "timeout-logger";
    public static final String JOURNAL_ADDRESS = "journal";
    public static final String TIMEOUT_LOG_COUNT_ADDRESS = "timeout-logger-count";
    private TimeoutLogRepository timeoutLogRepository;
    private JournalRepository journalRepository;


    public void stop() {
        journalRepository.dispose();
        timeoutLogRepository.dispose();
    }

    public void start(final Future<Void> startedResult) {
        try {
            setupJournal(container.config().getObject("journaler_config", new JsonObject()));
            setupTimeoutLog(container.config().getObject("timeout_log_config", new JsonObject()));
        } catch (RocksDBException e) {
            container.logger().error(e);
            startedResult.setFailure(e);
            return;
        }

        startedResult.setResult(null);
    }

    private void setupTimeoutLog(JsonObject config) throws RocksDBException {
        final String dbPath = config.getString("db_path", "timeout_log");
        timeoutLogRepository = new TimeoutLogRepository(dbPath);

        vertx.eventBus().registerLocalHandler(TIMEOUT_LOG_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message<Buffer>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<Buffer> msg) {

                final Buffer envelope = msg.body();
                Message message = Message.fromEnvelope(envelope);
                final byte[] key = message.getMessageId().getBytes();
                try {
                    timeoutLogRepository.put(key, envelope.getBytes());
                } catch (RocksDBException e) {
                    container.logger().error(e);
                }
            }
        });

        vertx.eventBus().registerLocalHandler(TIMEOUT_LOG_COUNT_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message event) {

                int count = 0;
                try {
                    count = timeoutLogRepository.getCount();
                } catch (RocksDBException e) {
                    container.logger().error(e);
                }
                finally {
                    event.reply(count);
                }
            }
        });
    }

    private void setupJournal(JsonObject config) throws RocksDBException {
        final String dbPath = config.getString("db_path", "journal");
        journalRepository = new JournalRepository(dbPath);

        vertx.eventBus().registerLocalHandler(JOURNAL_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message<Buffer>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<Buffer> msg) {
                final Buffer envelope = msg.body();
                Message message = io.cozmic.usherprotocols.core.Message.fromEnvelope(envelope);
                //TODO: temp key until we refine strategy for store/replay
                final byte[] key = ByteBuffer.allocate(8).putLong(Calendar.getInstance().getTimeInMillis()).array();
                try {
                    journalRepository.put(key, envelope.getBytes());
                } catch (RocksDBException e) {
                    container.logger().error(e);
                }
            }
        });
    }
}