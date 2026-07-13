package com.redis4j.storage.memory;

import com.redis4j.storage.snapshot.SnapshotEntry;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** 一次写命令执行前的键快照，用于在内存限制或持久化失败时恢复原始状态。 */
public record WriteBackup(Set<String> keys, Map<String, SnapshotEntry> captured) {
    public static final WriteBackup EMPTY = new WriteBackup(Set.of(), Map.of());

    public WriteBackup {
        keys = Set.copyOf(keys);
        captured = Map.copyOf(captured);
    }

    public static WriteBackup capture(MemoryManagedStore store, Set<String> keys) {
        if (keys.isEmpty()) return EMPTY;
        return new WriteBackup(keys, store.captureKeys(keys));
    }

    /**
     * 合并执行过程中新增的备份。键重叠时保留当前备份中的值，确保最终恢复到命令执行前，
     * 而不是恢复到命令执行后的中间状态。
     */
    public WriteBackup merge(WriteBackup additional) {
        if (additional.keys().isEmpty()) return this;
        if (keys.isEmpty()) return additional;
        Set<String> mergedKeys = new LinkedHashSet<>(keys);
        mergedKeys.addAll(additional.keys());
        Map<String, SnapshotEntry> mergedCaptured = new HashMap<>(captured);
        additional.captured().forEach((key, value) -> {
            if (!keys.contains(key)) mergedCaptured.put(key, value);
        });
        return new WriteBackup(mergedKeys, mergedCaptured);
    }

    public void restore(MemoryManagedStore store) {
        if (!keys.isEmpty()) store.restoreKeys(keys, captured);
    }
}
