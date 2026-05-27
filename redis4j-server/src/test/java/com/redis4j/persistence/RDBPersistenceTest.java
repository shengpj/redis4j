package com.redis4j.persistence;

import com.redis4j.storage.DataStore;
import com.redis4j.storage.MemoryStore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;

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

        Path rdbFile = tempDir.resolve("dump.rdb");
        writer.save(store, rdbFile.toString());

        DataStore loaded = new MemoryStore();
        reader.load(loaded, rdbFile.toString());

        assertEquals(4, loaded.dbSize());
        assertEquals("hello", loaded.get("string:key"));
        assertEquals(2, loaded.lLen("list:key"));
        assertEquals("value", loaded.hGet("hash:key", "field"));
        assertEquals(1, loaded.sCard("set:key"));
        loaded.close();
    }

    @Test
    void testLoadNonExistentFile() throws IOException {
        DataStore loaded = new MemoryStore();
        reader.load(loaded, tempDir.resolve("nonexistent.rdb").toString());
        assertEquals(0, loaded.dbSize());
        loaded.close();
    }
}
