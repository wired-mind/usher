package io.cozmic.usher.peristence;

import io.cozmic.usherprotocols.core.Request;
import org.rocksdb.*;

import java.util.Map;

/**
 * Created by chuck on 9/26/14.
 */
public class JournalRepository extends TimeSeqRepository {

    public JournalRepository(String dbPath) throws RocksDBException {
        super(dbPath);
    }

    public JournalRepository() throws RocksDBException {
        super("journal");
    }





}