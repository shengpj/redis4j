package com.redis4j.storage.memory;

import com.redis4j.command.WriteCommandSupport;
import com.redis4j.storage.DataStore;
import java.util.List;
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

    public void recordAccess(String commandName, String[] args) {
        if (isEnabled()) store.recordAccess(WriteCommandSupport.keys(commandName, args));
    }

    public EnforcementResult enforce(String commandName, WriteBackup backup) {
        if (!isEnabled() || WriteCommandSupport.isGuaranteedNonGrowing(commandName)) {
            return EnforcementResult.success(List.of(), backup);
        }
        if (store.estimatedMemoryUsage() <= maximumBytes) {
            return EnforcementResult.success(List.of(), backup);
        }
        EvictionPlan plan = store.planEvictions(maximumBytes, policy);
        if (!plan.sufficient()) {
            backup.restore(store);
            return EnforcementResult.rejected();
        }
        WriteBackup completeBackup = backup;
        if (!plan.keys().isEmpty()) {
            completeBackup = backup.merge(WriteBackup.capture(store, Set.copyOf(plan.keys())));
            dataStore.del(plan.keys().toArray(new String[0]));
        }
        return EnforcementResult.success(plan.keys(), completeBackup);
    }

    public long estimatedMemoryUsage() {
        return isEnabled() ? store.estimatedMemoryUsage() : 0;
    }

    public record EnforcementResult(boolean accepted, List<String> evictedKeys, WriteBackup backup) {
        public EnforcementResult {
            evictedKeys = List.copyOf(evictedKeys);
        }
        private static EnforcementResult success(List<String> keys, WriteBackup backup) {
            return new EnforcementResult(true, keys, backup);
        }
        private static EnforcementResult rejected() {
            return new EnforcementResult(false, List.of(), WriteBackup.EMPTY);
        }
    }
}
