package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import com.redis4j.storage.snapshot.SnapshotProvider;

import java.io.IOException;

public interface PersistenceEngine {
    void save(SnapshotProvider snapshotProvider, String filePath) throws IOException;
    void load(DataStore dataStore, String filePath) throws IOException;
    long getLastSaveTimestamp();
}
