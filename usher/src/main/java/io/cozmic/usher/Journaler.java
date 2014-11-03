package io.cozmic.usher;

import io.cozmic.usherprotocols.core.Message;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.platform.Verticle;

import java.io.Console;
import java.nio.ByteBuffer;
import java.util.Calendar;

/**
 * Created by chuck on 9/26/14.
 */
public class Journaler extends Verticle {
    public static final String ADDRESS = "journaler";
    private Options options;
    private RocksDB db;


    public void stop() {
        if (db != null) db.close();
        if (options != null) options.dispose();
    }

    public void start(final Future<Void> startedResult) {
        container.logger().info("lib path " + System.getProperty("java.library.path"));

        final String dbPath = container.config().getString("db_path", "journal");
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

        vertx.eventBus().registerLocalHandler(ADDRESS, new Handler<org.vertx.java.core.eventbus.Message<Buffer>>() {
            @Override
            public void handle(org.vertx.java.core.eventbus.Message<Buffer> msg) {
                final Buffer envelope = msg.body();
                Message message = io.cozmic.usherprotocols.core.Message.fromEnvelope(envelope);
                //TODO: temp key until we refine strategy for store/replay
                final byte[] key = ByteBuffer.allocate(8).putLong(Calendar.getInstance().getTimeInMillis()).array();
                try {
                    db.put(key, envelope.getBytes());
                } catch (RocksDBException e) {
                    container.logger().error(e);
                }
            }
        });

        startedResult.setResult(null);

    }
}