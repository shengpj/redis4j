package com.redis4j.storage;

/**
 * Single-partition in-memory store.
 *
 * <p>The command semantics live in {@link PartitionedMemoryStore}; using one
 * partition keeps the simple store and the partitioned store behavior aligned.</p>
 */
public final class MemoryStore extends PartitionedMemoryStore {
    public MemoryStore() {
        super(1);
    }
}
