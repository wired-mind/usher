package io.cozmic.usher.peristence;

import org.rocksdb.*;

/**
 * Created by chuck on 11/4/14.
 */
public class TimeoutLogRepository extends TimeSeqRepository {


    public TimeoutLogRepository() throws RocksDBException {
        super("timeouts");
    }

    public long getCount() throws RocksDBException {
        long count = 0;
        final RocksIterator iterator = db.newIterator();

        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            iterator.status();
            count++;
        }
        return count;
    }


}
