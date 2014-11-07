package io.cozmic.usher.peristence;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Created by chuck on 11/4/14.
 */
public class TimeoutLogRepository {
    private Options options;
    private RocksDB db;


    public TimeoutLogRepository(String dbPath) throws RocksDBException {
        RocksDB.loadLibrary();
        options = new Options().setCreateIfMissing(true);

        // a factory method that returns a RocksDB instance
        db = RocksDB.open(options, dbPath);
    }

    public void dispose() {
        if (db != null) db.close();
        if (options != null) options.dispose();
    }

    public void put(byte[] key, byte[] value) throws RocksDBException {
        db.put(key, value);
    }

    public int getCount() throws RocksDBException {
        int count = 0;
        final RocksIterator iterator = db.newIterator();
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            iterator.status();
            count++;
        }
        return count;
    }
}
