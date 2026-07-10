package com.redis4j.storage;

import com.redis4j.storage.snapshot.DataSnapshot;
import com.redis4j.storage.snapshot.SnapshotEntry;
import com.redis4j.storage.snapshot.SnapshotProvider;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

/** Aggregate storage capability used by the server composition root. */
public interface DataStore extends StringStore, KeyStore, ListStore, HashStore,
        SetStore, SnapshotProvider, AutoCloseable {

    Set<String> getAllKeys();

    @Override
    void close();

    @Override
    default DataSnapshot createSnapshot() {
        long now = System.currentTimeMillis();
        var entries = new ArrayList<SnapshotEntry>();
        for (String key : getAllKeys()) {
            long ttl = pttl(key);
            if (ttl == -2) continue;
            long expireAt = ttl < 0 ? -1 : now + ttl;
            DataType type = type(key);
            Object value = switch (type) {
                case STRING -> get(key);
                case LIST -> java.util.List.of(lRange(key, 0, -1));
                case SET -> new HashSet<>(sMembers(key));
                case HASH -> new LinkedHashMap<>(hGetAll(key));
                default -> null;
            };
            if (value != null) entries.add(new SnapshotEntry(key, type, value, expireAt));
        }
        return new DataSnapshot(entries);
    }
}
