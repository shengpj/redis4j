package com.redis4j.storage.memory;

import com.redis4j.command.CommandRegistry;
import com.redis4j.persistence.aof.AofFlushPolicy;
import com.redis4j.persistence.aof.AofManager;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.storage.DataStore;
import com.redis4j.storage.MemoryStore;
import com.redis4j.storage.PartitionedMemoryStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class MemoryEvictionTest {
    @TempDir
    Path tempDir;

    @Test
    void noEvictionRollsBackRejectedOverwrite() {
        try (MemoryStore store = new MemoryStore()) {
            store.set("key", "original");
            long limit = store.estimatedMemoryUsage() + 64;
            CommandRegistry registry = registry(store, limit, EvictionPolicy.NOEVICTION);

            CommandResponse response = registry.execute("SET", new String[]{"key", "x".repeat(1_000)});

            assertInstanceOf(CommandResponse.Error.class, response);
            assertEquals("original", store.get("key"));
            assertTrue(store.estimatedMemoryUsage() <= limit);
        }
    }

    @Test
    void noEvictionRollsBackCollectionMutation() {
        try (MemoryStore store = new MemoryStore()) {
            store.rPush("list", "original");
            long limit = store.estimatedMemoryUsage() + 64;
            CommandRegistry registry = registry(store, limit, EvictionPolicy.NOEVICTION);

            CommandResponse response = registry.execute("RPUSH", new String[]{"list", "x".repeat(1_000)});

            assertInstanceOf(CommandResponse.Error.class, response);
            assertArrayEquals(new String[]{"original"}, store.lRange("list", 0, -1));
        }
    }

    @Test
    void allKeysLruEvictsLeastRecentlyUsedKey() {
        try (MemoryStore sizing = new MemoryStore()) {
            sizing.set("sample", "x".repeat(200));
            long perKey = sizing.estimatedMemoryUsage();
            try (MemoryStore store = new MemoryStore()) {
                CommandRegistry registry = registry(store, perKey * 2 + 128, EvictionPolicy.ALLKEYS_LRU);
                assertSuccess(registry.execute("SET", new String[]{"old", "a".repeat(200)}));
                assertSuccess(registry.execute("SET", new String[]{"recent", "b".repeat(200)}));
                assertEquals("b".repeat(200), ((CommandResponse.BulkString)
                        registry.execute("GET", new String[]{"recent"})).value());

                assertSuccess(registry.execute("SET", new String[]{"new", "c".repeat(200)}));

                assertNull(store.get("old"));
                assertNotNull(store.get("recent"));
                assertNotNull(store.get("new"));
            }
        }
    }

    @Test
    void volatileTtlEvictsKeyWithNearestDeadline() {
        try (MemoryStore sizing = new MemoryStore()) {
            sizing.setEx("sample", "x".repeat(200), 60);
            long perKey = sizing.estimatedMemoryUsage();
            try (MemoryStore store = new MemoryStore()) {
                CommandRegistry registry = registry(store, perKey * 2 + 256, EvictionPolicy.VOLATILE_TTL);
                assertSuccess(registry.execute("SETEX", new String[]{"soon", "60", "a".repeat(200)}));
                assertSuccess(registry.execute("SETEX", new String[]{"later", "120", "b".repeat(200)}));

                assertSuccess(registry.execute("SET", new String[]{"persistent", "c".repeat(200)}));

                assertNull(store.get("soon"));
                assertNotNull(store.get("later"));
                assertNotNull(store.get("persistent"));
            }
        }
    }

    @Test
    void volatilePolicyRejectsWriteWhenNoExpiringKeyCanFreeEnoughMemory() {
        try (MemoryStore store = new MemoryStore()) {
            store.set("existing", "a".repeat(200));
            long limit = store.estimatedMemoryUsage() + 64;
            CommandRegistry registry = registry(store, limit, EvictionPolicy.VOLATILE_LRU);

            CommandResponse response = registry.execute("SET", new String[]{"new", "b".repeat(200)});

            assertInstanceOf(CommandResponse.Error.class, response);
            assertNotNull(store.get("existing"));
            assertNull(store.get("new"));
        }
    }

    @Test
    void evictionsArePersistedAtomicallyWithTheTriggeringCommand() throws Exception {
        Path file = tempDir.resolve("eviction.aof");
        long limit;
        try (MemoryStore sizing = new MemoryStore()) {
            sizing.set("sample", "x".repeat(200));
            limit = sizing.estimatedMemoryUsage() * 2 + 128;
        }

        try (MemoryStore store = new MemoryStore();
             AofManager aof = new AofManager(file, AofFlushPolicy.ALWAYS, 64)) {
            CommandRegistry registry = registry(store, limit, EvictionPolicy.ALLKEYS_LRU);
            aof.start();
            registry.setCommandJournal(aof);
            assertSuccess(registry.execute("SET", new String[]{"old", "a".repeat(200)}));
            assertSuccess(registry.execute("SET", new String[]{"recent", "b".repeat(200)}));
            registry.execute("GET", new String[]{"recent"});
            assertSuccess(registry.execute("SET", new String[]{"new", "c".repeat(200)}));
            registry.setCommandJournal(null);
            assertNull(store.get("old"));
        }

        try (DataStore restored = new MemoryStore()) {
            new AofManager(file, AofFlushPolicy.NO, 16).recover(new CommandRegistry(restored));
            assertNull(restored.get("old"));
            assertNotNull(restored.get("recent"));
            assertNotNull(restored.get("new"));
        }
    }

    @Test
    void partitionedStoreKeepsConcurrentWritesWithinLimit() throws Exception {
        long limit;
        try (MemoryStore sizing = new MemoryStore()) {
            sizing.set("sample", "x".repeat(100));
            limit = sizing.estimatedMemoryUsage() * 10;
        }
        try (PartitionedMemoryStore store = new PartitionedMemoryStore(4)) {
            CommandRegistry registry = registry(store, limit, EvictionPolicy.ALLKEYS_RANDOM);
            ExecutorService executor = Executors.newFixedThreadPool(4);
            try {
                List<Callable<CommandResponse>> writes = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    int index = i;
                    writes.add(() -> registry.execute("SET",
                            new String[]{"key-" + index, "v".repeat(100)}));
                }
                List<Future<CommandResponse>> futures = executor.invokeAll(writes);
                for (Future<CommandResponse> future : futures) assertSuccess(future.get());
            } finally {
                executor.shutdownNow();
            }
            assertTrue(store.estimatedMemoryUsage() <= limit);
            assertTrue(store.dbSize() < 100);
        }
    }

    private static CommandRegistry registry(DataStore store, long limit, EvictionPolicy policy) {
        CommandRegistry registry = new CommandRegistry(store);
        registry.setMemoryLimitManager(new MemoryLimitManager(store, limit, policy));
        return registry;
    }

    private static void assertSuccess(CommandResponse response) {
        assertFalse(response instanceof CommandResponse.Error,
                () -> "Unexpected command error: " + response);
    }
}
