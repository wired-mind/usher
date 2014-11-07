package io.cozmic.usher.peristence;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Created by chuck on 9/26/14.
 */
public class JournalRepository {
    public static final String ADDRESS = "journaler";
    private Options options;
    private RocksDB db;


    public JournalRepository(String dbPath) throws RocksDBException {
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

}