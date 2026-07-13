package com.redis4j.command.impl;

import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.storage.MemoryStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SortedSetCommandsTest {

    @Test
    void addRangeScoreAndRanksFollowScoreThenMemberOrder() {
        try (MemoryStore store = new MemoryStore()) {
            CommandRegistry commands = new CommandRegistry(store);
            assertInteger(3, commands.execute("ZADD",
                    new String[]{"board", "20", "bob", "10", "alice", "20", "amy"}));
            assertInteger(0, commands.execute("ZADD", new String[]{"board", "5", "bob"}));

            assertEquals(List.of("bob", "5", "alice", "10", "amy", "20"),
                    array(commands.execute("ZRANGE", new String[]{"board", "0", "-1", "WITHSCORES"})));
            assertEquals(List.of("amy", "alice"),
                    array(commands.execute("ZREVRANGE", new String[]{"board", "0", "1"})));
            assertInteger(2, commands.execute("ZRANK", new String[]{"board", "amy"}));
            assertInteger(0, commands.execute("ZREVRANK", new String[]{"board", "amy"}));
            assertBulk("10", commands.execute("ZSCORE", new String[]{"board", "alice"}));
            assertInteger(3, commands.execute("ZCARD", new String[]{"board"}));

            commands.execute("ZADD", new String[]{"tiny", "1e-300", "member"});
            assertBulk("1e-300", commands.execute("ZSCORE", new String[]{"tiny", "member"}));
        }
    }

    @Test
    void incrementAndScoreRangesSupportExclusiveBoundsAndLimit() {
        try (MemoryStore store = new MemoryStore()) {
            CommandRegistry commands = new CommandRegistry(store);
            commands.execute("ZADD", new String[]{"z", "1", "one", "2", "two", "3", "three", "4", "four"});

            assertBulk("2.5", commands.execute("ZINCRBY", new String[]{"z", "1.5", "one"}));
            assertInteger(2, commands.execute("ZCOUNT", new String[]{"z", "(2", "3"}));
            assertEquals(List.of("one", "2.5"), array(commands.execute("ZRANGEBYSCORE",
                    new String[]{"z", "-inf", "+inf", "LIMIT", "1", "1", "WITHSCORES"})));
        }
    }

    @Test
    void zscanReturnsMemberScorePairsAndWritesAreAtomicOnInvalidScores() {
        try (MemoryStore store = new MemoryStore()) {
            CommandRegistry commands = new CommandRegistry(store);
            CommandResponse invalid = commands.execute("ZADD",
                    new String[]{"invalid", "1", "ok", "NaN", "bad"});
            assertInstanceOf(CommandResponse.Error.class, invalid);
            assertEquals(0, store.zCard("invalid"));

            commands.execute("ZADD", new String[]{"z", "1", "alpha", "2", "beta"});
            CommandResponse.ArrayValue outer = assertInstanceOf(CommandResponse.ArrayValue.class,
                    commands.execute("ZSCAN", new String[]{"z", "0", "MATCH", "a*", "COUNT", "10"}));
            assertEquals("0", ((CommandResponse.BulkString) outer.values().get(0)).value());
            assertEquals(List.of("alpha", "1"), array(outer.values().get(1)));
        }
    }

    @Test
    void sortedSetCommandsRejectWrongTypesAndInvalidSyntax() {
        try (MemoryStore store = new MemoryStore()) {
            store.set("string", "value");
            CommandRegistry commands = new CommandRegistry(store);
            assertInstanceOf(CommandResponse.Error.class,
                    commands.execute("ZRANGE", new String[]{"string", "0", "-1"}));
            assertInstanceOf(CommandResponse.Error.class,
                    commands.execute("ZADD", new String[]{"z", "1", "one", "2"}));
            assertInstanceOf(CommandResponse.Error.class,
                    commands.execute("ZRANGEBYSCORE", new String[]{"z", "0", "1", "LIMIT", "-1", "2"}));
        }
    }

    private static List<String> array(CommandResponse response) {
        CommandResponse.ArrayValue values = assertInstanceOf(CommandResponse.ArrayValue.class, response);
        List<String> result = new ArrayList<>();
        for (CommandResponse value : values.values()) {
            result.add(assertInstanceOf(CommandResponse.BulkString.class, value).value());
        }
        return result;
    }

    private static void assertInteger(long expected, CommandResponse response) {
        assertEquals(expected, assertInstanceOf(CommandResponse.IntegerValue.class, response).value());
    }

    private static void assertBulk(String expected, CommandResponse response) {
        assertEquals(expected, assertInstanceOf(CommandResponse.BulkString.class, response).value());
    }
}
