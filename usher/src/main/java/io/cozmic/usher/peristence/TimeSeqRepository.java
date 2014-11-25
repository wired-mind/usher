package io.cozmic.usher.peristence;


import com.google.common.primitives.UnsignedBytes;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by chuck on 11/20/14.
 */
public class TimeSeqRepository {
    protected final BloomFilter filter;
    protected final Options options;
    protected final RocksDB db;

    static final AtomicLong seqCounter;
    static {
        seqCounter = new AtomicLong(0);
    }
    private final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

    public TimeSeqRepository(String dbPath) throws RocksDBException {
        RocksDB.loadLibrary();
        filter = new BloomFilter(16);
        options = new Options()
                .setFilter(filter)
                .setCreateIfMissing(true);
        db = RocksDB.open(options, dbPath);

        final RocksIterator iterator = db.newIterator();
        iterator.seekToLast();
        long currentSeq = 0;
        if (iterator.isValid() && SeqKey.isValid(iterator.key())) {
            final SeqKey key = new SeqKey(iterator.key());
            currentSeq = key.getSequence();
        }

        seqCounter.set(currentSeq);
    }

    public void dispose() {
        if (db != null) db.close();
        if (options != null) options.dispose();
        if (filter != null) filter.dispose();
    }


    public long seqValue() {
        return seqCounter.get();
    }





    public long count() throws RocksDBException {
        long count = 0;
        final RocksIterator iterator = db.newIterator();

        for (iterator.seek(SeqKey.FIRST); iterator.isValid(); iterator.next()) {
            iterator.status();
            count++;
        }
        iterator.dispose();
        return count;
    }

    public Map<SeqKey, byte[]> all() throws RocksDBException {
        final LinkedHashMap<SeqKey, byte[]> items = new LinkedHashMap<>();
        final RocksIterator iterator = db.newIterator();

        for (iterator.seek(SeqKey.FIRST); iterator.isValid(); iterator.next()) {
            iterator.status();
            final SeqKey key = new SeqKey(iterator.key());
            items.put(key, iterator.value());
        }
        iterator.dispose();
        return items;
    }



    public Map<TimeSeqKey, byte[]> allTimestamps() throws RocksDBException {
        return byTimeRange(0, -1);
    }

    public Map<TimeSeqKey, byte[]> byTimeRange(long start, long end) throws RocksDBException {
        final TimeSeqKey startKey = TimeSeqKey.build(start, 0);
        final TimeSeqKey endKey = TimeSeqKey.build(end, -1);

        return byRange(startKey, endKey);
    }

    public TimeSeqKey insert(byte[] value) throws RocksDBException {
        long seq;
        long timestamp;
        synchronized (seqCounter) {
            timestamp = System.currentTimeMillis();
            seq = seqCounter.incrementAndGet();
        }
        return insert(timestamp, seq, value);
    }


    /**
     * Please don't use this overload because the timestamp and sequence won't be synchronized
     * @param timestamp
     * @param value
     * @return
     * @throws RocksDBException
     */
    public TimeSeqKey insert(long timestamp, byte[] value) throws RocksDBException {
        return insert(timestamp, seqCounter.incrementAndGet(), value);
    }

    /**
     * Please don't use this overload because the timestamp and sequence won't be synchronized.
     * Allows for testing using specific timestamps
     * @param timestamp
     * @param sequence
     * @param value
     * @return
     * @throws RocksDBException
     */
    public TimeSeqKey insert(long timestamp, long sequence, byte[] value) throws RocksDBException {
        TimeSeqKey timeSeqKey = TimeSeqKey.build(timestamp, sequence);
        SeqKey seqKey = SeqKey.build(sequence);
        final WriteBatch batch = new WriteBatch();
        batch.put(timeSeqKey.bytes(), new byte[] {});
        batch.put(seqKey.bytes(), value);
        db.write(new WriteOptions(), batch);
        return timeSeqKey;
    }

    public Map<TimeSeqKey, byte[]> byRange(TimeSeqKey startKey, TimeSeqKey endKey) throws RocksDBException {
        final RocksIterator iterator = db.newIterator();

        final LinkedHashMap<TimeSeqKey, byte[]> items = new LinkedHashMap<>();
        for (iterator.seek(startKey.bytes()); iterator.isValid() && comparator.compare(iterator.key(), endKey.bytes()) < 1; iterator.next()) {
            iterator.status();
            final TimeSeqKey key = new TimeSeqKey(iterator.key());
            items.put(key, iterator.value());
        }
        iterator.dispose();
        return items;
    }

    public Map<SeqKey, byte[]> byRange(SeqKey startKey, SeqKey endKey) throws RocksDBException {
        final RocksIterator iterator = db.newIterator();

        final LinkedHashMap<SeqKey, byte[]> items = new LinkedHashMap<>();
        for (iterator.seek(startKey.bytes()); iterator.isValid() && comparator.compare(iterator.key(), endKey.bytes()) < 1; iterator.next()) {
            iterator.status();
            final SeqKey key = new SeqKey(iterator.key());
            items.put(key, iterator.value());
        }
        iterator.dispose();
        return items;
    }

    /**
     * Given a timestamp, what is the first sequence number occurring at or after this timestamp
     *
     * @param timestamp
     * @return
     * @throws RocksDBException
     */
    public TimeSeqKey minSeqFromTime(long timestamp) throws RocksDBException {
        final TimeSeqKey startKey = TimeSeqKey.build(timestamp, 0);

        final RocksIterator iterator = db.newIterator();
        iterator.seek(startKey.bytes());
        iterator.status();
        TimeSeqKey key = null;
        if (iterator.isValid()) {
            key = new TimeSeqKey(iterator.key());
        }
        iterator.dispose();
        return key;
    }

    /**
     * Given a timestamp, what is the highest sequence number occurring at or before this timestamp
     *
     * @param timestamp
     * @return
     * @throws RocksDBException
     */
    public TimeSeqKey maxSeqFromTime(long timestamp) throws RocksDBException {
        final TimeSeqKey startKey = TimeSeqKey.build(timestamp, 0);
        final TimeSeqKey endKey = TimeSeqKey.build(timestamp, -1);
        final RocksIterator iterator = db.newIterator();
        TimeSeqKey key = null;
        for (iterator.seek(startKey.bytes()); iterator.isValid() && comparator.compare(iterator.key(), endKey.bytes()) < 1; iterator.next()) {
            iterator.status();
            key = new TimeSeqKey(iterator.key());
        }
        iterator.dispose();
        return key;
    }

    private void debug(byte[] bytes) {
        StringBuilder builder = new StringBuilder("Debug ");
        for (int i = 0; i < bytes.length; ++i) {

            builder.append(" " + bytes[i] + " ");
        }
        System.out.println(builder.toString());
    }



    public static class TimeSeqKey {
        public static final byte TIME_SEQ_PREFIX =  0;
        public static final byte[] FIRST = new byte[] { TIME_SEQ_PREFIX };
        private final ByteBuffer key;

        public TimeSeqKey(byte[] value) {
            key = ByteBuffer.wrap(value);
        }

        public TimeSeqKey(ByteBuffer key) {
            this.key = key;
        }

        public static TimeSeqKey build(long timestamp, long sequence) {
            final ByteBuffer byteBuffer = ByteBuffer.allocate(17).put(TIME_SEQ_PREFIX).putLong(timestamp).putLong(sequence);
            return new TimeSeqKey(byteBuffer);
        }

        public long getSeq() {
            key.rewind();
            return key.getLong(9);
        }

        public byte[] bytes() {
            return key.array();
        }



        @Override
        public boolean equals(Object aThat) {
            if (this == aThat) return true;
            if (aThat instanceof TimeSeqKey) {
                TimeSeqKey that = (TimeSeqKey) aThat;
                return Arrays.equals(this.bytes(), that.bytes());
            } else if (aThat instanceof SeqKey) {
                SeqKey that = (SeqKey) aThat;
                return getSeqKey().equals(that);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return getSeqKey().hashCode();
        }

        public long getTimestamp() {
            key.rewind();
            return key.getLong(1);
        }

        public SeqKey getSeqKey() {
            return SeqKey.build(getSeq());
        }
    }

    public static class SeqKey {
        public static final byte SEQ_PREFIX = 127;
        public static final byte[] FIRST = new byte[]{SeqKey.SEQ_PREFIX};
        private final ByteBuffer key;

        public SeqKey(ByteBuffer key) {

            this.key = key;
        }

        public SeqKey(byte[] value) {
            key = ByteBuffer.wrap(value);
        }

        public static SeqKey build(long sequence) {
            final ByteBuffer byteBuffer = ByteBuffer.allocate(9).put(SEQ_PREFIX).putLong(sequence);
            return new SeqKey(byteBuffer);
        }

        public static boolean isValid(byte[] key) {
            return key.length == 9 && key[0] == SEQ_PREFIX;
        }

        public byte[] bytes() {
            return key.array();
        }

        @Override
        public boolean equals(Object aThat) {
            if (this == aThat) return true;
            if (aThat instanceof SeqKey) {
                SeqKey that = (SeqKey) aThat;
                return Arrays.equals(this.bytes(), that.bytes());
            } else if (aThat instanceof TimeSeqKey) {
                TimeSeqKey that = (TimeSeqKey) aThat;
                SeqKey thatSeqKey = that.getSeqKey();
                return Arrays.equals(this.bytes(), thatSeqKey.bytes());
            } else {
                return false;
            }

        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes());
        }

        public long getSequence() {
            key.rewind();
            return key.getLong(1);
        }

    }


}
