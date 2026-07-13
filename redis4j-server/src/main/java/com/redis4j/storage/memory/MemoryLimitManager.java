package com.redis4j.storage.memory;

import com.redis4j.command.WriteCommandSupport;
import com.redis4j.storage.DataStore;
import com.redis4j.storage.snapshot.SnapshotEntry;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** 在写命令临界区内执行内存检查、回滚和淘汰。 */
public final class MemoryLimitManager {
    private final DataStore dataStore;
    private final MemoryManagedStore store;
    private final long maximumBytes;
    private final EvictionPolicy policy;

    public MemoryLimitManager(DataStore dataStore, long maximumBytes, EvictionPolicy policy) {
        if (maximumBytes < 0) throw new IllegalArgumentException("maxmemory cannot be negative");
        this.dataStore = dataStore;
        this.maximumBytes = maximumBytes;
        this.policy = policy == null ? EvictionPolicy.NOEVICTION : policy;
        if (maximumBytes > 0 && !(dataStore instanceof MemoryManagedStore)) {
            throw new IllegalArgumentException("Configured DataStore does not support maxmemory");
        }
        this.store = maximumBytes > 0 ? (MemoryManagedStore) dataStore : null;
    }

    public boolean isEnabled() {
        return maximumBytes > 0;
    }

    public boolean isWriteCommand(String commandName) {
        return WriteCommandSupport.isWriteCommand(commandName);
    }

    public WriteBackup capture(String commandName, String[] args) {
        if (!isEnabled()) return WriteBackup.EMPTY;
        Set<String> keys = WriteCommandSupport.keys(commandName, args);
        return new WriteBackup(keys, store.captureKeys(keys));
    }

    public void recordAccess(String commandName, String[] args) {
        if (isEnabled()) store.recordAccess(WriteCommandSupport.keys(commandName, args));
    }

    public EnforcementResult enforce(String commandName, WriteBackup backup) {
        if (!isEnabled() || WriteCommandSupport.isGuaranteedNonGrowing(commandName)) {
            return EnforcementResult.success(List.of());
        }
        if (store.estimatedMemoryUsage() <= maximumBytes) return EnforcementResult.success(List.of());
        EvictionPlan plan = store.planEvictions(maximumBytes, policy);
        if (!plan.sufficient()) {
            store.restoreKeys(backup.keys(), backup.captured());
            return EnforcementResult.rejected();
        }
        if (!plan.keys().isEmpty()) dataStore.del(plan.keys().toArray(new String[0]));
        return EnforcementResult.success(plan.keys());
    }

    public long estimatedMemoryUsage() {
        return isEnabled() ? store.estimatedMemoryUsage() : 0;
    }

    public record WriteBackup(Set<String> keys, Map<String, SnapshotEntry> captured) {
        private static final WriteBackup EMPTY = new WriteBackup(Set.of(), Map.of());
        public WriteBackup {
            keys = Set.copyOf(keys);
            captured = Map.copyOf(captured);
        }
    }

    public record EnforcementResult(boolean accepted, List<String> evictedKeys) {
        public EnforcementResult {
            evictedKeys = List.copyOf(evictedKeys);
        }
        private static EnforcementResult success(List<String> keys) { return new EnforcementResult(true, keys); }
        private static EnforcementResult rejected() { return new EnforcementResult(false, List.of()); }
    }
}
