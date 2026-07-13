package com.redis4j.storage.memory;

import com.redis4j.storage.snapshot.SnapshotEntry;

import java.util.Map;
import java.util.Set;

/** 内存限制控制器需要的存储级能力。 */
public interface MemoryManagedStore {
    long estimatedMemoryUsage();

    Map<String, SnapshotEntry> captureKeys(Set<String> keys);

    void restoreKeys(Set<String> keys, Map<String, SnapshotEntry> captured);

    void recordAccess(Set<String> keys);

    EvictionPlan planEvictions(long maximumBytes, EvictionPolicy policy);
}
