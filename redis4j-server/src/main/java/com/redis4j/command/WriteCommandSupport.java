package com.redis4j.command;

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/** 写命令识别及命令键提取，供 AOF 与内存限制共同使用。 */
public final class WriteCommandSupport {
    private static final Set<String> WRITE_COMMANDS = Set.of(
            "SET", "SETNX", "SETEX", "MSET", "INCR", "INCRBY", "DECR", "DECRBY", "APPEND",
            "DEL", "EXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "RENAME", "FLUSHDB", "FLUSHALL",
            "LPUSH", "RPUSH", "LPOP", "RPOP", "LSET", "LTRIM",
            "HSET", "HSETNX", "HDEL", "HMSET", "HINCRBY",
            "SADD", "SREM", "SMOVE", "SPOP", "ZADD", "ZREM", "ZINCRBY");
    private static final Set<String> FIRST_KEY_COMMANDS = Set.of(
            "GET", "SET", "SETNX", "SETEX", "INCR", "INCRBY", "DECR", "DECRBY", "STRLEN", "APPEND",
            "EXPIRE", "EXPIREAT", "PEXPIREAT", "TTL", "PTTL", "PERSIST", "TYPE",
            "LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE", "LSET", "LTRIM", "LINDEX",
            "HSET", "HSETNX", "HGET", "HGETALL", "HDEL", "HEXISTS", "HLEN", "HKEYS", "HVALS",
            "HMSET", "HMGET", "HINCRBY", "HSCAN", "SADD", "SREM", "SISMEMBER", "SCARD", "SMEMBERS",
            "SPOP", "SRANDMEMBER", "SSCAN", "ZADD", "ZREM", "ZSCORE", "ZCARD", "ZINCRBY",
            "ZRANGE", "ZREVRANGE", "ZRANK", "ZREVRANK", "ZCOUNT", "ZRANGEBYSCORE", "ZSCAN");
    private static final Set<String> ALL_KEY_COMMANDS = Set.of("MGET", "DEL", "EXISTS", "SINTER", "SUNION", "SDIFF");
    private static final Set<String> NON_GROWING_WRITES = Set.of(
            "DEL", "PERSIST", "FLUSHDB", "FLUSHALL", "LPOP", "RPOP", "LTRIM", "HDEL", "SREM", "SPOP",
            "ZREM");

    private WriteCommandSupport() {}

    public static boolean isWriteCommand(String commandName) {
        return WRITE_COMMANDS.contains(normalize(commandName));
    }

    public static boolean isGuaranteedNonGrowing(String commandName) {
        return NON_GROWING_WRITES.contains(normalize(commandName));
    }

    public static Set<String> keys(String commandName, String[] args) {
        String command = normalize(commandName);
        LinkedHashSet<String> keys = new LinkedHashSet<>();
        if (args == null || args.length == 0) return keys;
        if ("MSET".equals(command)) {
            for (int i = 0; i < args.length; i += 2) keys.add(args[i]);
        } else if (ALL_KEY_COMMANDS.contains(command)) {
            for (String arg : args) keys.add(arg);
        } else if ("RENAME".equals(command) || "SMOVE".equals(command)) {
            keys.add(args[0]);
            if (args.length > 1) keys.add(args[1]);
        } else if (FIRST_KEY_COMMANDS.contains(command)) {
            keys.add(args[0]);
        }
        return keys;
    }

    private static String normalize(String commandName) {
        return commandName == null ? "" : commandName.toUpperCase(Locale.ROOT);
    }
}
