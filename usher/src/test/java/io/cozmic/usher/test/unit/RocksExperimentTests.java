package io.cozmic.usher.test.unit;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;
import org.rocksdb.*;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;

/**
 * Created by chuck on 11/20/14.
 */
public class RocksExperimentTests {

    @Test
    public void testMerge() throws RocksDBException {
        RocksDB.loadLibrary();
        BloomFilter filter = new BloomFilter(10);
        Options options = new Options()
                .setFilter(filter)
                .setCreateIfMissing(true);

        // a factory method that returns a RocksDB
        RocksDB db = RocksDB.open(options, "test6276");



        db.put(longKey(0), "zz".getBytes());
        db.put(longKey(1), "zz".getBytes());
        db.put(longKey(2), "zz".getBytes());
        db.put(longKey(3), "zz".getBytes());
        db.put(longKey(4), "zz".getBytes());
        db.put(longKey(5), "zz".getBytes());
        db.put(longKey(6), "zz".getBytes());
        db.put(longKey(7), "zz".getBytes());
        db.put(longKey(8), "zz2".getBytes());
        db.put(longKey(9), "zz2".getBytes());
        db.put(longKey(10), "zz2".getBytes());
        db.put(longKey(-1), "zz2".getBytes());
        db.put(longKey(-100), "zz2".getBytes());
        db.put(longKey(-2), "zz2".getBytes());

        db.put(stringKey("0key2"), "zz2".getBytes());
        db.put(stringKey("1key2"), "zz2".getBytes());
        db.put(stringKey("2key2"), "zz2".getBytes());
        db.put(stringKey("9key2"), "zz2".getBytes());
        db.put(stringKey("10key2"), "zz2".getBytes());
        db.put(stringKey("akey2"), "zz2".getBytes());
        db.put(stringKey("zkey2"), "zz2".getBytes());

        final RocksIterator iterator = db.newIterator();

        for (iterator.seek(new byte[]{127}); iterator.isValid(); iterator.next()) {
            iterator.status();
            final byte[] key = iterator.key();
            if (key[0] == 0x0) {
                System.out.println(new String(ArrayUtils.subarray(key, 1, key.length - 1)));
            } else {
                final long aLong = ByteBuffer.allocate(9).put(key).getLong(1);
                System.out.println(aLong);
            }
        }

//        iterator.seekToLast();
//        final byte[] key = iterator.key();
//        final boolean valid = iterator.isValid();
//        final String x = new String(key);
//
//        iterator.seekToFirst();
//        final byte[] first = iterator.key();
//        final String firstKey = new String(first);
//
//        System.out.println(x);
    }

    private byte[] stringKey(String val) {
        return ArrayUtils.addAll(new byte[] {0x0}, val.getBytes());
    }

    private byte[] longKey(long value) {
        final byte[] longBytes = ByteBuffer.allocate(8).putLong(value).array();
        return ArrayUtils.addAll(new byte[] {127}, longBytes);
    }
}
