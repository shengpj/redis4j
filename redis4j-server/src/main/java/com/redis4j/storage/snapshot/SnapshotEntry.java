package com.redis4j.storage.snapshot;

import com.redis4j.storage.DataType;

/** Immutable representation of one key at snapshot time. */
public record SnapshotEntry(String key, DataType type, Object value, long expireAt) {
}
