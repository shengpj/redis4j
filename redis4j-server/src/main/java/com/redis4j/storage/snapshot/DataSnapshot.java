package com.redis4j.storage.snapshot;

import java.util.List;

public record DataSnapshot(List<SnapshotEntry> entries) {
    public DataSnapshot {
        entries = List.copyOf(entries);
    }
}
