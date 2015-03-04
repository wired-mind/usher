package io.cozmic.usher.test.unit;

import io.cozmic.usher.peristence.JournalRepository;
import io.cozmic.usher.peristence.TimeSeqRepository;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Created by chuck on 11/20/14.
 */
public class RepositoryTests {

    @Test
    public void testSequenceInitialization() throws RocksDBException {
        final String dbPath = testDbPath();
        TimeSeqRepository repository = new TimeSeqRepository(dbPath);

        assertEquals(0, repository.seqValue());

        repository.insert("hi".getBytes());
        assertEquals(1, repository.seqValue());
        repository.dispose();

        repository = new TimeSeqRepository(dbPath);
        assertEquals(1, repository.seqValue());
    }

    @Test
    public void testCount() throws RocksDBException {
        final String dbPath = testDbPath();
        TimeSeqRepository repository = new TimeSeqRepository(dbPath);

        repository.insert("hello".getBytes());
        repository.insert("world".getBytes());

        assertEquals(2, repository.seqValue());

        assertEquals(2, repository.count());
    }

    @Test
    public void testFetchAll() throws RocksDBException {
        final String dbPath = testDbPath();
        TimeSeqRepository repository = new TimeSeqRepository(dbPath);

        final TimeSeqRepository.TimeSeqKey key1 = repository.insert("hello".getBytes());
        final TimeSeqRepository.TimeSeqKey key2 = repository.insert("world".getBytes());

        assertEquals(2, repository.seqValue());

        final Map<TimeSeqRepository.SeqKey, byte[]> list = repository.all();
        assertEquals(2, list.size());

        assertArrayEquals("hello".getBytes(), list.get(key1));
        assertArrayEquals("world".getBytes(), list.get(key2));
    }

    @Test
    public void testKeyEquals() {
        final TimeSeqRepository.TimeSeqKey key1 = TimeSeqRepository.TimeSeqKey.build(1, 1);
        final TimeSeqRepository.TimeSeqKey key2 = TimeSeqRepository.TimeSeqKey.build(1, 1);
        final TimeSeqRepository.TimeSeqKey key3 = TimeSeqRepository.TimeSeqKey.build(1, 2);

        assertEquals(key1, key2);
        assertNotEquals(key1, key3);
    }

    /**
     * Not really a test. just for my experimenting
     * @throws RocksDBException
     */
    @Test
    public void testOrdering() throws RocksDBException {
        final String dbPath = testDbPath();
        JournalRepository repository = new JournalRepository(dbPath);



        for (int i = 0; i < 100; i++) {
            repository.insert(i, "hello".getBytes());
        }

        repository.insert(-1416416364412l, "hello".getBytes());
        repository.insert(-4, "hello".getBytes());
        repository.insert(1416416364412l, "hello".getBytes());
        repository.insert(1416416364412l, "hello".getBytes());
        repository.insert(1416416364413l, "hello".getBytes());
        repository.insert(1416416364513l, "hello".getBytes());
        repository.insert(1416416364113l, "hello".getBytes());
        repository.insert(-1, "hello".getBytes());
        repository.insert(Long.MIN_VALUE, "hello".getBytes());
        repository.insert(Long.MAX_VALUE, "hello".getBytes());

        final Map<TimeSeqRepository.TimeSeqKey, byte[]> list = repository.allTimestamps();
        for (TimeSeqRepository.TimeSeqKey timeSeqKey : list.keySet()) {
            System.out.println(timeSeqKey.getTimestamp());
        }

    }

    @Test
    public void testTimestamps() throws RocksDBException {
        final String dbPath = testDbPath();
        JournalRepository repository = new JournalRepository(dbPath);


        repository.insert(-1416416364432l, "hello".getBytes());
        repository.insert(1416416364412l, "hello".getBytes());
        repository.insert(1416416364422l, "world".getBytes());
        repository.insert(1416416364432l, "worldworld".getBytes());
        repository.insert(1416416365412l, "worldworld".getBytes());
        repository.insert(1416416363412l, "worldworld".getBytes());



        final Map<TimeSeqRepository.TimeSeqKey, byte[]> howNegativesWork = repository.byTimeRange(1416416365412l, -1416416364432l);

        assertEquals(2, howNegativesWork.size());

        final Map<TimeSeqRepository.TimeSeqKey, byte[]> list = repository.byTimeRange(1416416364412l, 1416416364432l);

        assertEquals(3, list.size());
    }

    @Test
    public void testMinMaxAtTime() throws RocksDBException {
        final String dbPath = testDbPath();
        TimeSeqRepository repository = new TimeSeqRepository(dbPath);


        repository.insert(-1416416364432l, "hello".getBytes());
        repository.insert(1416416364412l, "hello".getBytes());
        repository.insert(1416416364422l, "world".getBytes());
        repository.insert(1416416364422l, "world".getBytes());

        repository.insert(1416416364432l, "worldworld".getBytes());
        repository.insert(1416416365412l, "worldworld".getBytes());
        repository.insert(1416416363412l, "worldworld".getBytes());
        repository.insert(1416416364422l, "world".getBytes());


        TimeSeqRepository.TimeSeqKey minKey = repository.minSeqFromTime(1416416364422l);
        TimeSeqRepository.TimeSeqKey maxKey = repository.maxSeqFromTime(1416416364422l);

        assertEquals(3, minKey.getSeq());
        assertEquals(8, maxKey.getSeq());
    }

    @Test
    public void testRange() throws RocksDBException {
        final String dbPath = testDbPath();
        TimeSeqRepository repository = new TimeSeqRepository(dbPath);


        long timestamp = 0;
        repository.insert(timestamp++, "hello".getBytes());
        repository.insert(timestamp++, "hello".getBytes());
        final TimeSeqRepository.TimeSeqKey key3 = repository.insert(timestamp++, "world2".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        final TimeSeqRepository.TimeSeqKey key5 = repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());
        repository.insert(timestamp++, "worldworld".getBytes());

        final long end = System.currentTimeMillis();

        final Map<TimeSeqRepository.SeqKey, byte[]> list = repository.byRange(TimeSeqRepository.SeqKey.build(3), TimeSeqRepository.SeqKey.build(5));

        assertEquals(3, list.size());
        final byte[] thirdRow = list.get(key3);
        assertEquals("world2", new String(thirdRow));
    }

    private String testDbPath() {
        final File dir = new File("testdb");
        if (!dir.exists())
            dir.mkdir();
        return Paths.get(dir.getAbsolutePath(), UUID.randomUUID().toString()).toString();
    }
}
