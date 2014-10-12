package io.cozmic.usher;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.platform.Verticle;

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

        RocksDB.loadLibrary();
        options = new Options().setCreateIfMissing(true);
        try {
            // a factory method that returns a RocksDB instance
            db = RocksDB.open(options, "/Volumes/Master/Users/chuck/Code/MadSwan/WiredMind/splitscnd/cozmic.io/db");
            // do something
        } catch (RocksDBException e) {
            container.logger().error(e);
            startedResult.setFailure(e);
            return;
        }

        vertx.eventBus().registerHandler(ADDRESS, new Handler<Message<Buffer>>() {
            @Override
            public void handle(Message<Buffer> msg) {
                //TODO: temp key until we refine strategy for store/replay
                final byte[] key = ByteBuffer.allocate(8).putLong(Calendar.getInstance().getTimeInMillis()).array();
                try {
                    db.put(key, msg.body().getBytes());
                } catch (RocksDBException e) {
                    container.logger().error(e);
                }
            }
        }, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                if (event.failed()) {
                    container.logger().error(event.cause());
                    startedResult.setFailure(event.cause());
                    return;
                }

                container.logger().info("The Journaler is ready!!!!");
                startedResult.setResult(null);
            }
        });

    }
}