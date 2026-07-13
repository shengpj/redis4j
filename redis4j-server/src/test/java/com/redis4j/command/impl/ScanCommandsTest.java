package com.redis4j.command.impl;

import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.storage.MemoryStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ScanCommandsTest {

    @Test
    void scanIteratesWithMatchAndCountUntilCursorReturnsToZero() {
        try (MemoryStore store = new MemoryStore()) {
            store.set("user:1", "a");
            store.set("order:1", "b");
            store.set("user:2", "c");
            CommandRegistry registry = new CommandRegistry(store);

            Set<String> found = new HashSet<>();
            String cursor = "0";
            do {
                Page page = page(registry.execute("SCAN",
                        new String[]{cursor, "MATCH", "user:*", "COUNT", "1"}));
                cursor = page.cursor();
                found.addAll(page.values());
            } while (!"0".equals(cursor));

            assertEquals(Set.of("user:1", "user:2"), found);
        }
    }

    @Test
    void sscanAndHscanReturnRedisShapedNestedArrays() {
        try (MemoryStore store = new MemoryStore()) {
            store.sAdd("set", "alpha", "beta", "alpine");
            store.hSet("hash", "first", "1");
            store.hSet("hash", "second", "2");
            CommandRegistry registry = new CommandRegistry(store);

            Page setPage = page(registry.execute("SSCAN",
                    new String[]{"set", "0", "MATCH", "al*", "COUNT", "10"}));
            assertEquals("0", setPage.cursor());
            assertEquals(Set.of("alpha", "alpine"), new HashSet<>(setPage.values()));

            Page hashPage = page(registry.execute("HSCAN",
                    new String[]{"hash", "0", "MATCH", "s*", "COUNT", "10"}));
            assertEquals("0", hashPage.cursor());
            assertEquals(List.of("second", "2"), hashPage.values());
        }
    }

    @Test
    void scanRejectsInvalidCursorAndCount() {
        try (MemoryStore store = new MemoryStore()) {
            CommandRegistry registry = new CommandRegistry(store);
            assertInstanceOf(CommandResponse.Error.class,
                    registry.execute("SCAN", new String[]{"invalid"}));
            assertInstanceOf(CommandResponse.Error.class,
                    registry.execute("SCAN", new String[]{"0", "COUNT", "0"}));
            assertInstanceOf(CommandResponse.Error.class,
                    registry.execute("SCAN", new String[]{"0", "UNKNOWN", "1"}));
        }
    }

    @Test
    void scanSkipsExpiredKeysAndMayReturnAnEmptyIntermediatePage() {
        try (MemoryStore store = new MemoryStore()) {
            store.set("expired", "value");
            store.expire("expired", 0);
            store.set("other", "value");
            store.set("target", "value");
            CommandRegistry registry = new CommandRegistry(store);

            Page first = page(registry.execute("SCAN",
                    new String[]{"0", "MATCH", "target", "COUNT", "1"}));
            assertNotEquals("0", first.cursor());
            assertTrue(first.values().isEmpty());

            Page second = page(registry.execute("SCAN",
                    new String[]{first.cursor(), "MATCH", "target", "COUNT", "1"}));
            assertEquals("0", second.cursor());
            assertEquals(List.of("target"), second.values());
        }
    }

    @Test
    void collectionScansReturnAnEmptyPageForMissingKeys() {
        try (MemoryStore store = new MemoryStore()) {
            CommandRegistry registry = new CommandRegistry(store);
            Page setPage = page(registry.execute("SSCAN", new String[]{"missing", "0"}));
            Page hashPage = page(registry.execute("HSCAN", new String[]{"missing", "0"}));

            assertEquals(new Page("0", List.of()), setPage);
            assertEquals(new Page("0", List.of()), hashPage);
        }
    }

    private static Page page(CommandResponse response) {
        CommandResponse.ArrayValue outer = assertInstanceOf(CommandResponse.ArrayValue.class, response);
        assertNotNull(outer.values());
        assertEquals(2, outer.values().size());
        String cursor = assertInstanceOf(CommandResponse.BulkString.class, outer.values().get(0)).value();
        CommandResponse.ArrayValue values = assertInstanceOf(CommandResponse.ArrayValue.class, outer.values().get(1));
        List<String> strings = new ArrayList<>();
        for (CommandResponse value : values.values()) {
            strings.add(assertInstanceOf(CommandResponse.BulkString.class, value).value());
        }
        return new Page(cursor, strings);
    }

    private record Page(String cursor, List<String> values) {}
}
