package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import com.redis4j.storage.snapshot.SnapshotProvider;

import java.io.IOException;

public final class RdbPersistenceEngine implements PersistenceEngine {
    private final RDBWriter writer;
    private final RDBReader reader;

    public RdbPersistenceEngine() {
        this(new RDBWriter(), new RDBReader());
    }

    RdbPersistenceEngine(RDBWriter writer, RDBReader reader) {
        this.writer = writer;
        this.reader = reader;
    }

    @Override public void save(SnapshotProvider provider, String path) throws IOException { writer.save(provider, path); }
    @Override public void load(DataStore store, String path) throws IOException { reader.load(store, path); }
    @Override public long getLastSaveTimestamp() { return reader.getLastSaveTimestamp(); }
}
