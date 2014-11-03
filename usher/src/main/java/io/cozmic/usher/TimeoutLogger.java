package io.cozmic.usher;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.platform.Verticle;

import io.cozmic.usherprotocols.core.Message;

/**
 * Created by chuck on 10/31/14.
 */
public class TimeoutLogger extends Verticle {
    public static final String LOG_ADDRESS = "timeout-logger";
    public static final String COUNT_ADDRESS = "timeout-logger-count";
    private Options options;
    private RocksDB db;


    public void stop() {
        if (db != null) db.close();
        if (options != null) options.dispose();
    }

    public void start(final Future<Void> startedResult) {

        final String dbPath = container.config().getString("db_path", "timeout_log");
        RocksDB.loadLibrary();
        options = new Options().setCreateIfMissing(true);
        try {
            // a factory method that returns a RocksDB instance
            db = RocksDB.open(options, dbPath);
            // do something
        } catch (RocksDBException e) {
            container.logger().error(e);
            startedResult.setFailure(e);
            return;
        }

        vertx.eventBus().registerLocalHandler(LOG_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message<Buffer>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<Buffer> msg) {

                final Buffer envelope = msg.body();
                Message message = Message.fromEnvelope(envelope);
                final byte[] key = message.getMessageId().getBytes();
                try {
                    db.put(key, envelope.getBytes());
                } catch (RocksDBException e) {
                    container.logger().error(e);
                }
            }
        });

        vertx.eventBus().registerLocalHandler(COUNT_ADDRESS, new Handler<org.vertx.java.core.eventbus.Message>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message event) {
                int count = 0;
                try {
                final RocksIterator iterator = db.newIterator();
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    iterator.status();
                    count++;

                }
                } catch (RocksDBException e) {
                    e.printStackTrace();
                }
                finally {
                    event.reply(count);
                }
            }
        });

        startedResult.setResult(null);
    }
}