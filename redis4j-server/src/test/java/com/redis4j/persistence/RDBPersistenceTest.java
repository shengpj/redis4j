package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import com.redis4j.storage.MemoryStore;
import com.redis4j.storage.DataType;
import com.redis4j.storage.snapshot.DataSnapshot;
import com.redis4j.storage.snapshot.SnapshotEntry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RDBPersistenceTest {

    @TempDir
    Path tempDir;

    private RDBWriter writer;
    private RDBReader reader;
    private DataStore store;

    @BeforeEach
    void setUp() {
        writer = new RDBWriter();
        reader = new RDBReader();
        store = new MemoryStore();
    }

    @AfterEach
    void tearDown() {
        store.close();
    }

    @Test
    void testStringPersistence() throws IOException {
        store.set("name", "redis4j");
        store.set("port", "6379");

        Path rdbFile = tempDir.resolve("dump.rdb");
        writer.save(store, rdbFile.toString());

        DataStore loaded = new MemoryStore();
        reader.load(loaded, rdbFile.toString());

        assertEquals("redis4j", loaded.get("name"));
        assertEquals("6379", loaded.get("port"));
        assertEquals(2, loaded.dbSize());
        loaded.close();
    }

    @Test
    void testListPersistence() throws IOException {
        store.rPush("mylist", "a", "b", "c");

        Path rdbFile = tempDir.resolve("dump.rdb");
        writer.save(store, rdbFile.toString());

        DataStore loaded = new MemoryStore();
        reader.load(loaded, rdbFile.toString());

        assertEquals(3, loaded.lLen("mylist"));
        assertArrayEquals(new String[]{"a", "b", "c"}, loaded.lRange("mylist", 0, -1));
        loaded.close();
    }

    @Test
    void testHashPersistence() throws IOException {
        store.hSet("user:1", "name", "Alice");
        store.hSet("user:1", "age", "30");

        Path rdbFile = tempDir.resolve("dump.rdb");
        writer.save(store, rdbFile.toString());

        DataStore loaded = new MemoryStore();
        reader.load(loaded, rdbFile.toString());

        assertEquals("Alice", loaded.hGet("user:1", "name"));
        assertEquals("30", loaded.hGet("user:1", "age"));
        loaded.close();
    }

    @Test
    void testSetPersistence() throws IOException {
        store.sAdd("tags", "java", "redis", "netty");

        Path rdbFile = tempDir.resolve("dump.rdb");
        writer.save(store, rdbFile.toString());

        DataStore loaded = new MemoryStore();
        reader.load(loaded, rdbFile.toString());

        assertEquals(3, loaded.sCard("tags"));
        assertTrue(loaded.sIsMember("tags", "java"));
        loaded.close();
    }

    @Test
    void testTTLRestore() throws IOException {
        store.setEx("temp", "data", 3600);

        Path rdbFile = tempDir.resolve("dump.rdb");
        writer.save(store, rdbFile.toString());

        DataStore loaded = new MemoryStore();
        reader.load(loaded, rdbFile.toString());

        assertEquals("data", loaded.get("temp"));
        long ttl = loaded.ttl("temp");
        assertTrue(ttl > 0 && ttl <= 3600);
        loaded.close();
    }

    @Test
    void testMixedDataTypes() throws IOException {
        store.set("string:key", "hello");
        store.rPush("list:key", "item1", "item2");
        store.hSet("hash:key", "field", "value");
        store.sAdd("set:key", "member");
        store.zAdd("zset:key", java.util.Map.of("member", 1.5));

        Path rdbFile = tempDir.resolve("dump.rdb");
        writer.save(store, rdbFile.toString());

        DataStore loaded = new MemoryStore();
        reader.load(loaded, rdbFile.toString());

        assertEquals(5, loaded.dbSize());
        assertEquals("hello", loaded.get("string:key"));
        assertEquals(2, loaded.lLen("list:key"));
        assertEquals("value", loaded.hGet("hash:key", "field"));
        assertEquals(1, loaded.sCard("set:key"));
        assertEquals(1.5, loaded.zScore("zset:key", "member"));
        loaded.close();
    }

    @Test
    void testLoadNonExistentFile() throws IOException {
        DataStore loaded = new MemoryStore();
        reader.load(loaded, tempDir.resolve("nonexistent.rdb").toString());
        assertEquals(0, loaded.dbSize());
        loaded.close();
    }

    @Test
    void testSortedSetPersistence() throws IOException {
        store.zAdd("leaderboard", java.util.Map.of("alice", 10.25, "bob", 7.5));

        Path rdbFile = tempDir.resolve("zset.rdb");
        writer.save(store, rdbFile.toString());

        try (DataStore loaded = new MemoryStore()) {
            reader.load(loaded, rdbFile.toString());
            assertEquals(2, loaded.zCard("leaderboard"));
            assertEquals(10.25, loaded.zScore("leaderboard", "alice"));
            assertEquals(List.of("bob", "alice"), loaded.zRange("leaderboard", 0, -1, false)
                    .stream().map(com.redis4j.storage.ZSetStore.ScoredMember::member).toList());
        }
    }

    @Test
    void expiredEntriesAreNotResurrected() throws Exception {
        store.set("string", "value");
        store.rPush("list", "value");
        store.expireMs("string", 200);
        store.expireMs("list", 200);
        Path rdbFile = tempDir.resolve("expired.rdb");
        writer.save(store, rdbFile.toString());
        Thread.sleep(250);

        try (DataStore loaded = new MemoryStore()) {
            reader.load(loaded, rdbFile.toString());
            assertFalse(loaded.exists("string"));
            assertFalse(loaded.exists("list"));
        }
    }

    @Test
    void corruptedFileDoesNotPartiallyMutateStore() throws Exception {
        Path rdbFile = tempDir.resolve("corrupted.rdb");
        Files.write(rdbFile, "REDIS0011".getBytes());
        try (DataStore loaded = new MemoryStore()) {
            loaded.set("existing", "safe");
            assertThrows(IOException.class, () -> reader.load(loaded, rdbFile.toString()));
            assertEquals("safe", loaded.get("existing"));
            assertEquals(1, loaded.dbSize());
        }
    }

    @Test
    void failedSaveKeepsPreviousSnapshot() throws Exception {
        Path rdbFile = tempDir.resolve("atomic.rdb");
        store.set("stable", "value");
        writer.save(store, rdbFile.toString());

        DataSnapshot invalid = new DataSnapshot(List.of(
                new SnapshotEntry("broken", DataType.STRING, 123, -1)));
        assertThrows(ClassCastException.class, () -> writer.save(() -> invalid, rdbFile.toString()));

        try (DataStore loaded = new MemoryStore()) {
            reader.load(loaded, rdbFile.toString());
            assertEquals("value", loaded.get("stable"));
            assertFalse(loaded.exists("broken"));
        }
    }

    @Test
    void writesEntryThatEndsExactlyAtBufferBoundary() throws Exception {
        String key = "k".repeat(65_513);
        store.set(key, "value");
        Path rdbFile = tempDir.resolve("boundary.rdb");
        writer.save(store, rdbFile.toString());
        try (DataStore loaded = new MemoryStore()) {
            reader.load(loaded, rdbFile.toString());
            assertEquals("value", loaded.get(key));
        }
    }
}
