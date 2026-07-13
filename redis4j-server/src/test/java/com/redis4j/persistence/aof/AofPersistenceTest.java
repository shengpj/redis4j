package com.redis4j.persistence.aof;

import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.storage.DataStore;
import com.redis4j.storage.MemoryStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
            assertSuccess(registry.execute("SETEX", new String[]{"expiring", "60", "value"}));
            assertSuccess(registry.execute("SPOP", new String[]{"set"}));
            registry.setCommandJournal(null);
        }

        try (DataStore restored = new MemoryStore()) {
            CommandRegistry replayRegistry = new CommandRegistry(restored);
            AofManager reader = new AofManager(file, AofFlushPolicy.EVERYSEC, 128);
            assertTrue(reader.recover(replayRegistry) >= 6);
            assertEquals("value", restored.get("string"));
            assertArrayEquals(new String[]{"a", "b"}, restored.lRange("list", 0, -1));
            assertEquals("value", restored.hGet("hash", "field"));
            assertEquals(0, restored.sCard("set"));
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

    private static void assertSuccess(CommandResponse response) {
        assertFalse(response instanceof CommandResponse.Error,
                () -> "Unexpected command error: " + response);
    }
}
