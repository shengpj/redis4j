package com.redis4j.persistence.aof;

import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.storage.DataStore;
import com.redis4j.storage.MemoryStore;
import com.redis4j.storage.snapshot.SnapshotProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class AofPersistenceTest {
    @TempDir
    Path tempDir;

    @Test
    void restoresAllDataTypesAndAbsoluteTtl() throws Exception {
        Path file = tempDir.resolve("appendonly.aof");
        try (DataStore store = new MemoryStore();
             AofManager manager = new AofManager(file, AofFlushPolicy.ALWAYS, 128)) {
            CommandRegistry registry = new CommandRegistry(store);
            manager.start();
            registry.setCommandJournal(manager);

            assertSuccess(registry.execute("SET", new String[]{"string", "value"}));
            assertSuccess(registry.execute("RPUSH", new String[]{"list", "a", "b"}));
            assertSuccess(registry.execute("HSET", new String[]{"hash", "field", "value"}));
            assertSuccess(registry.execute("SADD", new String[]{"set", "member"}));
            assertSuccess(registry.execute("ZADD", new String[]{"zset", "1.5", "member"}));
            assertSuccess(registry.execute("ZINCRBY", new String[]{"zset", "0.5", "member"}));
            assertSuccess(registry.execute("SETEX", new String[]{"expiring", "60", "value"}));
            assertSuccess(registry.execute("SPOP", new String[]{"set"}));
            registry.setCommandJournal(null);
        }

        try (DataStore restored = new MemoryStore()) {
            CommandRegistry replayRegistry = new CommandRegistry(restored);
            AofManager reader = new AofManager(file, AofFlushPolicy.EVERYSEC, 128);
            assertTrue(reader.recover(replayRegistry) >= 8);
            assertEquals("value", restored.get("string"));
            assertArrayEquals(new String[]{"a", "b"}, restored.lRange("list", 0, -1));
            assertEquals("value", restored.hGet("hash", "field"));
            assertEquals(0, restored.sCard("set"));
            assertEquals(2.0, restored.zScore("zset", "member"));
            assertEquals("value", restored.get("expiring"));
            assertTrue(restored.pttl("expiring") > 0);
        }
    }

    @Test
    void truncatesIncompleteTailAndKeepsValidRecords() throws Exception {
        Path file = tempDir.resolve("truncated.aof");
        try (DataStore store = new MemoryStore();
             AofManager manager = new AofManager(file, AofFlushPolicy.ALWAYS, 16)) {
            CommandRegistry registry = new CommandRegistry(store);
            manager.start();
            registry.setCommandJournal(manager);
            assertSuccess(registry.execute("SET", new String[]{"key", "value"}));
            registry.setCommandJournal(null);
        }
        long validSize = Files.size(file);
        Files.write(file, new byte[]{1, 2, 3, 4, 5}, StandardOpenOption.APPEND);

        try (DataStore restored = new MemoryStore()) {
            AofManager reader = new AofManager(file, AofFlushPolicy.EVERYSEC, 16);
            reader.recover(new CommandRegistry(restored));
            assertEquals("value", restored.get("key"));
            assertEquals(validSize, Files.size(file));
        }
    }

    @Test
    void expiredSetExIsNotResurrectedDuringReplay() throws Exception {
        Path file = tempDir.resolve("expiry.aof");
        try (DataStore store = new MemoryStore();
             AofManager manager = new AofManager(file, AofFlushPolicy.ALWAYS, 16)) {
            CommandRegistry registry = new CommandRegistry(store);
            manager.start();
            registry.setCommandJournal(manager);
            assertSuccess(registry.execute("SETEX", new String[]{"temporary", "1", "value"}));
            registry.setCommandJournal(null);
        }
        Thread.sleep(1_100);
        try (DataStore restored = new MemoryStore()) {
            new AofManager(file, AofFlushPolicy.EVERYSEC, 16).recover(new CommandRegistry(restored));
            assertFalse(restored.exists("temporary"));
        }
    }

    @Test
    void concurrentWritesReplayInTheSameGlobalOrder() throws Exception {
        Path file = tempDir.resolve("concurrent.aof");
        int threads = 8;
        int increments = 250;
        try (DataStore store = new MemoryStore();
             AofManager manager = new AofManager(file, AofFlushPolicy.EVERYSEC, 64)) {
            CommandRegistry registry = new CommandRegistry(store);
            manager.start();
            registry.setCommandJournal(manager);
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    start.await();
                    for (int j = 0; j < increments; j++) {
                        assertSuccess(registry.execute("INCR", new String[]{"counter"}));
                    }
                    return null;
                });
            }
            start.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(20, TimeUnit.SECONDS));
            assertEquals(String.valueOf(threads * increments), store.get("counter"));
            registry.setCommandJournal(null);
        }

        try (DataStore restored = new MemoryStore()) {
            new AofManager(file, AofFlushPolicy.NO, 64).recover(new CommandRegistry(restored));
            assertEquals(String.valueOf(threads * increments), restored.get("counter"));
        }
    }

    @Test
    void supportsEveryFlushPolicy() throws Exception {
        for (AofFlushPolicy policy : AofFlushPolicy.values()) {
            Path file = tempDir.resolve(policy.name().toLowerCase() + ".aof");
            try (DataStore store = new MemoryStore();
                 AofManager manager = new AofManager(file, policy, 16)) {
                CommandRegistry registry = new CommandRegistry(store);
                manager.start();
                registry.setCommandJournal(manager);
                assertSuccess(registry.execute("SET", new String[]{"policy", policy.name()}));
                registry.setCommandJournal(null);
            }
            try (DataStore restored = new MemoryStore()) {
                new AofManager(file, policy, 16).recover(new CommandRegistry(restored));
                assertEquals(policy.name(), restored.get("policy"));
            }
        }
    }

    @Test
    void createsAofBaselineFromExistingSnapshot() throws Exception {
        Path file = tempDir.resolve("baseline.aof");
        try (DataStore existing = new MemoryStore();
             AofManager manager = new AofManager(file, AofFlushPolicy.ALWAYS, 16)) {
            existing.set("string", "value");
            existing.rPush("list", "a", "b");
            manager.start();
            manager.appendSnapshot(existing.createSnapshot());
        }
        try (DataStore restored = new MemoryStore()) {
            new AofManager(file, AofFlushPolicy.EVERYSEC, 16).recover(new CommandRegistry(restored));
            assertEquals("value", restored.get("string"));
            assertArrayEquals(new String[]{"a", "b"}, restored.lRange("list", 0, -1));
        }
    }

    @Test
    void backgroundRewriteCompactsHistoryAndKeepsConcurrentDelta() throws Exception {
        Path file = tempDir.resolve("rewrite.aof");
        long sizeBeforeRewrite;
        try (DataStore store = new MemoryStore();
             AofManager manager = new AofManager(file, AofFlushPolicy.EVERYSEC, 4096)) {
            manager.start();
            List<CompletableFuture<Void>> appends = new ArrayList<>();
            for (int i = 0; i < 10_000; i++) {
                String value = "value-" + i;
                store.set("key", value);
                appends.add(manager.append("SET", new String[]{"key", value},
                        new CommandResponse.SimpleString("OK")));
            }
            // 保留足够多的独立键，使快照写入阶段稳定存在，避免测试比后台线程慢而错过增量窗口。
            for (int i = 0; i < 1_000; i++) store.set("snapshot-key-" + i, "value-" + i);
            CompletableFuture.allOf(appends.toArray(new CompletableFuture[0])).get(20, TimeUnit.SECONDS);
            sizeBeforeRewrite = Files.size(file);
            assertTrue(manager.shouldAutoRewrite(1, 100));

            CommandRegistry registry = new CommandRegistry(store);
            registry.setCommandJournal(manager);
            CountDownLatch snapshotCreated = new CountDownLatch(1);
            CountDownLatch allowSnapshotReturn = new CountDownLatch(1);
            SnapshotProvider coordinatedSnapshot = () -> {
                var snapshot = store.createSnapshot();
                snapshotCreated.countDown();
                try {
                    if (!allowSnapshotReturn.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("timed out coordinating AOF rewrite test");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("AOF rewrite test was interrupted", e);
                }
                return snapshot;
            };
            assertTrue(manager.bgRewrite(registry, coordinatedSnapshot));
            assertTrue(snapshotCreated.await(5, TimeUnit.SECONDS));
            allowSnapshotReturn.countDown();
            assertSuccess(registry.execute("SET", new String[]{"during-rewrite", "preserved"}));
            waitUntil(() -> !manager.isRewriting(), 20_000);
            assertFalse(manager.shouldAutoRewrite(1, 100));
            registry.setCommandJournal(null);
        }

        assertTrue(Files.size(file) < sizeBeforeRewrite / 4,
                "rewritten AOF should be substantially smaller than command history");
        assertTrue(AofManager.isHybridFile(file));
        long validHybridSize = Files.size(file);
        Files.write(file, new byte[]{1, 2, 3}, StandardOpenOption.APPEND);
        try (DataStore restored = new MemoryStore()) {
            new AofManager(file, AofFlushPolicy.NO, 16).recover(new CommandRegistry(restored), restored);
            assertEquals("value-9999", restored.get("key"));
            assertEquals("preserved", restored.get("during-rewrite"));
        }
        assertEquals(validHybridSize, Files.size(file), "invalid hybrid AOF tail should be truncated");
    }

    @Test
    void canDisableRdbPreambleAndRewriteAsPureAof() throws Exception {
        Path file = tempDir.resolve("pure-rewrite.aof");
        try (DataStore store = new MemoryStore();
             AofManager manager = new AofManager(file, AofFlushPolicy.ALWAYS, 64, false)) {
            CommandRegistry registry = new CommandRegistry(store);
            manager.start();
            registry.setCommandJournal(manager);
            assertSuccess(registry.execute("SET", new String[]{"key", "value"}));
            assertTrue(manager.bgRewrite(registry, store));
            waitUntil(() -> !manager.isRewriting(), 5_000);
            registry.setCommandJournal(null);
        }

        assertFalse(AofManager.isHybridFile(file));
        try (DataStore restored = new MemoryStore()) {
            new AofManager(file, AofFlushPolicy.NO, 16, false).recover(new CommandRegistry(restored));
            assertEquals("value", restored.get("key"));
        }
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) Thread.sleep(10);
        assertTrue(condition.getAsBoolean(), "condition was not met before timeout");
    }

    private static void assertSuccess(CommandResponse response) {
        assertFalse(response instanceof CommandResponse.Error,
                () -> "Unexpected command error: " + response);
    }
}
