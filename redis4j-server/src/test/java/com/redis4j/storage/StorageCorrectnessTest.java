package com.redis4j.storage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class StorageCorrectnessTest {
    private static final List<Supplier<DataStore>> STORES = List.of(
            MemoryStore::new,
            () -> new PartitionedMemoryStore(4));

    @Test
    void ttlSentinelsAndHsetResultsMatchRedisSemantics() {
        for (Supplier<DataStore> factory : STORES) {
            try (DataStore store = factory.get()) {
                assertEquals(-2, store.ttl("missing"));
                store.set("persistent", "value");
                assertEquals(-1, store.ttl("persistent"));
                assertEquals(1, store.hSet("hash", "field", "one"));
                assertEquals(0, store.hSet("hash", "field", "two"));
            }
        }
    }

    @Test
    void persistentSetCannotBeDeletedByAnOldExpiryDeadline() throws Exception {
        for (Supplier<DataStore> factory : STORES) {
            try (DataStore store = factory.get()) {
                store.setEx("key", "old", 1);
                store.set("key", "new");
                Thread.sleep(1_200);
                assertEquals("new", store.get("key"));
                assertEquals(-1, store.ttl("key"));
            }
        }
    }

    @Test
    void expiredKeysDoNotBlockSetNx() throws Exception {
        for (Supplier<DataStore> factory : STORES) {
            try (DataStore store = factory.get()) {
                store.set("key", "old");
                store.expireMs("key", 1);
                Thread.sleep(10);
                assertTrue(store.setNx("key", "new"));
                assertEquals("new", store.get("key"));
            }
        }
    }

    @Test
    void concurrentHashUpdatesOnOneKeyDoNotLoseWrites() throws Exception {
        for (Supplier<DataStore> factory : STORES) {
            try (DataStore store = factory.get()) {
                int threads = 8;
                int increments = 500;
                ExecutorService executor = Executors.newFixedThreadPool(threads);
                CountDownLatch start = new CountDownLatch(1);
                for (int i = 0; i < threads; i++) {
                    executor.submit(() -> {
                        start.await();
                        for (int j = 0; j < increments; j++) store.hIncrBy("counter", "value", 1);
                        return null;
                    });
                }
                start.countDown();
                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
                assertEquals(String.valueOf(threads * increments), store.hGet("counter", "value"));
            }
        }
    }
}
