package io.cozmic.usher;

import io.cozmic.pulsar.provider.PulsarProviderServer;
import io.cozmic.usher.peristence.JournalRepository;
import io.cozmic.usher.peristence.TimeSeqRepository;
import io.cozmic.usherprotocols.core.Request;
import org.rocksdb.RocksDBException;
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import java.util.Map;

/**
 * Created by chuck on 10/31/14.
 */
public class PersistenceVerticle extends BusModBase {
    public static final String TIMEOUT_LOG_ADDRESS = "timeout-logger";
    public static final String JOURNAL_ADDRESS = "journal";
    private static final String JOURNAL_DELETE_ADDRESS = "journal-delete";
    public static final String TIMEOUT_LOG_COUNT_ADDRESS = "timeout-logger-count";
    public static final String TIMEOUT_LOG_LIST_ADDRESS = "timeout-logger-list";
    public static final String TIMEOUT_LOG_RANGE_QUERY_ADDRESS = "timeout-logger-range-query";
    public static final String TIMEOUT_LOG_DELETE_ADDRESS = "timeout-logger-delete";
    public static final String CONNECTION_LOG_ADDRESS = "connection-logger";
    private static final String CONNECTION_LOG_DELETE_ADDRESS = "connection-logger-delete";
    public static final long DEFAULT_TTL = 1000 * 60 * 60 * 24 * 180; //180 days
    public static final int EVICTION_PROCESS_PERIOD = 1000 * 60 * 60; //every hour
    private TimeSeqRepository timeoutLogRepository;
    private TimeSeqRepository connectionLogRepository;
    private JournalRepository journalRepository;


    public void stop() {
        journalRepository.dispose();
        timeoutLogRepository.dispose();
    }

    public void start(final Future<Void> startedResult) {
        start();
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

        setupEvictionTimer(config.getLong("ttl", DEFAULT_TTL), CONNECTION_LOG_DELETE_ADDRESS);

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

        /**
         * Ensure this is a local handler too.
         */
        vertx.eventBus().registerLocalHandler(CONNECTION_LOG_DELETE_ADDRESS, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> msg) {
                final JsonObject cmd = msg.body();
                final TimeSeqRepository.TimeSeqKey startKey = TimeSeqRepository.TimeSeqKey.build(cmd.getLong("startingTime"), cmd.getLong("startingSequence"));
                final TimeSeqRepository.TimeSeqKey endKey = TimeSeqRepository.TimeSeqKey.build(cmd.getLong("endingTime"), cmd.getLong("endingSequence"));

                try {
                    connectionLogRepository.deleteRange(startKey, endKey);
                    sendOK(msg);
                } catch (RocksDBException e) {
                    container.logger().error(e);
                    sendError(msg, "Error deleting from the connection log", e);
                }
            }
        });
    }

    private void setupTimeoutLog(JsonObject config) throws RocksDBException {
        final String dbPath = config.getString("dbPath", "timeouts");


        setupEvictionTimer(config.getLong("ttl", DEFAULT_TTL), TIMEOUT_LOG_DELETE_ADDRESS);

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
                    vertx.eventBus().send(msg.body(), e.getMessage());
                }
            }
        });

        vertx.eventBus().registerHandler(TIMEOUT_LOG_LIST_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message<String>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<String> msg) {
                container.logger().info("Received at " + TIMEOUT_LOG_LIST_ADDRESS + " value: " + msg.body());

                try {
                    Map<TimeSeqRepository.SeqKey, byte[]> messages = timeoutLogRepository.all();
                    for (TimeSeqRepository.SeqKey seqKey : messages.keySet()) {
                        final Request request = Request.fromEnvelope(messages.get(seqKey));
                        vertx.eventBus().send(msg.body(), String.format("Seq: %s Message: %s\n\n", seqKey.getSequence(), request.getBody().toString()));
                    }

                } catch (RocksDBException e) {
                    container.logger().error(e);
                    vertx.eventBus().send(msg.body(), e.getMessage());
                }
            }
        });

        /**
         * Ensure this is a local handler as it's used for pulsar timeout replay, which is node specific.
         *
         * It returns the start time/seq and end time/seq of the current timeout log.
         */
        vertx.eventBus().registerLocalHandler(TIMEOUT_LOG_RANGE_QUERY_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message<JsonObject>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<JsonObject> msg) {

                try {
                    //retrieves first key
                    final TimeSeqRepository.TimeSeqKey first = timeoutLogRepository.minSeqFromTime(0);
                    //retrieves last key
                    final TimeSeqRepository.TimeSeqKey last = timeoutLogRepository.maxSeqFromTime(-1);

                    final JsonObject response = buildRange(first, last);
                    sendStatus("ok", msg, response);


                } catch (RocksDBException e) {
                    container.logger().error(e);
                    sendError(msg, "Error querying the timeout log", e);
                }
            }
        });

        /**
         * Ensure this is a local handler too.
         */
        vertx.eventBus().registerLocalHandler(TIMEOUT_LOG_DELETE_ADDRESS, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> msg) {
                final JsonObject cmd = msg.body();
                final TimeSeqRepository.TimeSeqKey startKey = TimeSeqRepository.TimeSeqKey.build(cmd.getLong("startingTime"), cmd.getLong("startingSequence"));
                final TimeSeqRepository.TimeSeqKey endKey = TimeSeqRepository.TimeSeqKey.build(cmd.getLong("endingTime"), cmd.getLong("endingSequence"));

                try {
                    timeoutLogRepository.deleteRange(startKey, endKey);
                    sendOK(msg);
                } catch (RocksDBException e) {
                    container.logger().error(e);
                    sendError(msg, "Error deleting from the timeout log", e);
                }
            }
        });

        vertx.eventBus().registerLocalHandler(PulsarProviderServer.DEFAULT_REQUEST_PERSISTENCE_ADDRESS, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> msg) {
                final JsonObject cmd = msg.body();
                final Long startingSequence = cmd.getLong("startingSequence");
                final Long endingSequence = cmd.getLong("endingSequence");


                final JsonArray messages = new JsonArray();
                try {
                    final Map<TimeSeqRepository.SeqKey, byte[]> range = timeoutLogRepository.byRange(TimeSeqRepository.SeqKey.build(startingSequence), TimeSeqRepository.SeqKey.build(endingSequence));

                    for (TimeSeqRepository.SeqKey seqKey : range.keySet()) {
                        final JsonObject message = new JsonObject()
                                .putBinary("request", range.get(seqKey));
                        messages.addObject(message);
                    }

                } catch (RocksDBException e) {
                    container.logger().error("Failed to load timeouts.", e);
                }

                msg.reply(messages);
            }
        });
    }

    /**
     * Once an hour evict data (rocks eviction not yet ported to java)
     * @param ttl
     * @param deleteAddress
     */
    private void setupEvictionTimer(final Long ttl, final String deleteAddress) {
        vertx.setPeriodic(EVICTION_PROCESS_PERIOD, new Handler<Long>() {
            @Override
            public void handle(Long event) {

                final long evictionMax = System.currentTimeMillis() - ttl;
                vertx.eventBus().send(deleteAddress, buildRange(TimeSeqRepository.TimeSeqKey.MIN, TimeSeqRepository.TimeSeqKey.build(evictionMax, -1)));
            }
        });
    }

    private JsonObject buildRange(TimeSeqRepository.TimeSeqKey first, TimeSeqRepository.TimeSeqKey last) {
        return new JsonObject()
                                .putNumber("startingTime", first.getTimestamp())
                                .putNumber("startingSequence", first.getSeq())
                                .putNumber("endingTime", last.getTimestamp())
                                .putNumber("endingSequence", last.getSeq());
    }

    private void setupJournal(JsonObject config) throws RocksDBException {
        final String dbPath = config.getString("dbPath", "journal");

        setupEvictionTimer(config.getLong("ttl", DEFAULT_TTL), JOURNAL_DELETE_ADDRESS);

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

        /**
         * Ensure this is a local handler too.
         */
        vertx.eventBus().registerLocalHandler(JOURNAL_DELETE_ADDRESS, new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> msg) {
                final JsonObject cmd = msg.body();
                final TimeSeqRepository.TimeSeqKey startKey = TimeSeqRepository.TimeSeqKey.build(cmd.getLong("startingTime"), cmd.getLong("startingSequence"));
                final TimeSeqRepository.TimeSeqKey endKey = TimeSeqRepository.TimeSeqKey.build(cmd.getLong("endingTime"), cmd.getLong("endingSequence"));

                try {
                    journalRepository.deleteRange(startKey, endKey);
                    sendOK(msg);
                } catch (RocksDBException e) {
                    container.logger().error(e);
                    sendError(msg, "Error deleting from the journal", e);
                }
            }
        });
    }
}