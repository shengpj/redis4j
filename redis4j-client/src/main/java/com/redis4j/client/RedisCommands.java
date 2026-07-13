package com.redis4j.client;

import com.redis4j.protocol.RedisMessageHelper;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Redis 命令便捷封装
 */
public class RedisCommands {

    private final RedisClient client;

    public RedisCommands(RedisClient client) {
        this.client = client;
    }

    // ==================== String 操作 ====================

    public String get(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("GET", key);
        return getString(response);
    }

    public void set(String key, String value) throws InterruptedException {
        client.sendCommand("SET", key, value);
    }

    public void setEx(String key, String value, long seconds) throws InterruptedException {
        client.sendCommand("SETEX", key, String.valueOf(seconds), value);
    }

    public boolean setNx(String key, String value) throws InterruptedException {
        RedisMessage response = client.sendCommand("SETNX", key, value);
        return getInteger(response) == 1;
    }

    public String[] mGet(String... keys) throws InterruptedException {
        String[] command = new String[keys.length + 1];
        command[0] = "MGET";
        System.arraycopy(keys, 0, command, 1, keys.length);
        RedisMessage response = client.sendCommand(command);
        return getStringArray(response);
    }

    public void mSet(Map<String, String> map) throws InterruptedException {
        String[] command = new String[map.size() * 2 + 1];
        command[0] = "MSET";
        int i = 1;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            command[i++] = entry.getKey();
            command[i++] = entry.getValue();
        }
        client.sendCommand(command);
    }

    public long incr(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("INCR", key);
        return getInteger(response);
    }

    public long incrBy(String key, long delta) throws InterruptedException {
        RedisMessage response = client.sendCommand("INCRBY", key, String.valueOf(delta));
        return getInteger(response);
    }

    public long decr(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("DECR", key);
        return getInteger(response);
    }

    public long decrBy(String key, long delta) throws InterruptedException {
        RedisMessage response = client.sendCommand("DECRBY", key, String.valueOf(delta));
        return getInteger(response);
    }

    public long strlen(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("STRLEN", key);
        return getInteger(response);
    }

    public long append(String key, String value) throws InterruptedException {
        RedisMessage response = client.sendCommand("APPEND", key, value);
        return getInteger(response);
    }

    // ==================== Key 操作 ====================

    public long del(String... keys) throws InterruptedException {
        String[] command = new String[keys.length + 1];
        command[0] = "DEL";
        System.arraycopy(keys, 0, command, 1, keys.length);
        RedisMessage response = client.sendCommand(command);
        return getInteger(response);
    }

    public boolean exists(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("EXISTS", key);
        return getInteger(response) == 1;
    }

    public boolean expire(String key, long seconds) throws InterruptedException {
        RedisMessage response = client.sendCommand("EXPIRE", key, String.valueOf(seconds));
        return getInteger(response) == 1;
    }

    public long ttl(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("TTL", key);
        return getInteger(response);
    }

    public long pttl(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("PTTL", key);
        return getInteger(response);
    }

    public String type(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("TYPE", key);
        return getString(response);
    }

    public Set<String> keys(String pattern) throws InterruptedException {
        RedisMessage response = client.sendCommand("KEYS", pattern);
        return new HashSet<>(Arrays.asList(getStringArray(response)));
    }

    public long dbSize() throws InterruptedException {
        RedisMessage response = client.sendCommand("DBSIZE");
        return getInteger(response);
    }

    public void flushDb() throws InterruptedException {
        client.sendCommand("FLUSHDB");
    }

    public void flushAll() throws InterruptedException {
        client.sendCommand("FLUSHALL");
    }

    public void select(int index) throws InterruptedException {
        client.sendCommand("SELECT", String.valueOf(index));
    }

    public void rename(String oldKey, String newKey) throws InterruptedException {
        client.sendCommand("RENAME", oldKey, newKey);
    }

    public boolean persist(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("PERSIST", key);
        return getInteger(response) == 1;
    }

    public boolean expireAt(String key, long timestamp) throws InterruptedException {
        RedisMessage response = client.sendCommand("EXPIREAT", key, String.valueOf(timestamp));
        return getInteger(response) == 1;
    }

    public String time() throws InterruptedException {
        RedisMessage response = client.sendCommand("TIME");
        List<RedisMessage> array = RedisMessageHelper.extractArray(response);
        if (array == null || array.isEmpty()) return "(nil)";
        return String.join(" ", getStringArray(response));
    }

    public String ping() throws InterruptedException {
        RedisMessage response = client.sendCommand("PING");
        return getString(response);
    }

    public String echo(String message) throws InterruptedException {
        RedisMessage response = client.sendCommand("ECHO", message);
        return getString(response);
    }

    // ==================== List 操作 ====================

    public long lPush(String key, String... values) throws InterruptedException {
        String[] command = new String[values.length + 2];
        command[0] = "LPUSH";
        command[1] = key;
        System.arraycopy(values, 0, command, 2, values.length);
        RedisMessage response = client.sendCommand(command);
        return getInteger(response);
    }

    public long rPush(String key, String... values) throws InterruptedException {
        String[] command = new String[values.length + 2];
        command[0] = "RPUSH";
        command[1] = key;
        System.arraycopy(values, 0, command, 2, values.length);
        RedisMessage response = client.sendCommand(command);
        return getInteger(response);
    }

    public String lPop(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("LPOP", key);
        return getString(response);
    }

    public String rPop(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("RPOP", key);
        return getString(response);
    }

    public long lLen(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("LLEN", key);
        return getInteger(response);
    }

    public String[] lRange(String key, long start, long stop) throws InterruptedException {
        RedisMessage response = client.sendCommand("LRANGE", key, String.valueOf(start), String.valueOf(stop));
        return getStringArray(response);
    }

    // ==================== Hash 操作 ====================

    public long hSet(String key, String field, String value) throws InterruptedException {
        RedisMessage response = client.sendCommand("HSET", key, field, value);
        return getInteger(response);
    }

    public boolean hSetNx(String key, String field, String value) throws InterruptedException {
        RedisMessage response = client.sendCommand("HSETNX", key, field, value);
        return getInteger(response) == 1;
    }

    public String hGet(String key, String field) throws InterruptedException {
        RedisMessage response = client.sendCommand("HGET", key, field);
        return getString(response);
    }

    public Map<String, String> hGetAll(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("HGETALL", key);
        List<RedisMessage> array = RedisMessageHelper.extractArray(response);
        Map<String, String> map = new LinkedHashMap<>();
        if (array != null) {
            for (int i = 0; i < array.size() - 1; i += 2) {
                String k = getString(array.get(i));
                String v = getString(array.get(i + 1));
                map.put(k, v);
            }
        }
        return map;
    }

    public long hDel(String key, String... fields) throws InterruptedException {
        String[] command = new String[fields.length + 2];
        command[0] = "HDEL";
        command[1] = key;
        System.arraycopy(fields, 0, command, 2, fields.length);
        RedisMessage response = client.sendCommand(command);
        return getInteger(response);
    }

    public boolean hExists(String key, String field) throws InterruptedException {
        RedisMessage response = client.sendCommand("HEXISTS", key, field);
        return getInteger(response) == 1;
    }

    public long hLen(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("HLEN", key);
        return getInteger(response);
    }

    // ==================== Set 操作 ====================

    public long sAdd(String key, String... members) throws InterruptedException {
        String[] command = new String[members.length + 2];
        command[0] = "SADD";
        command[1] = key;
        System.arraycopy(members, 0, command, 2, members.length);
        RedisMessage response = client.sendCommand(command);
        return getInteger(response);
    }

    public long sRem(String key, String... members) throws InterruptedException {
        String[] command = new String[members.length + 2];
        command[0] = "SREM";
        command[1] = key;
        System.arraycopy(members, 0, command, 2, members.length);
        RedisMessage response = client.sendCommand(command);
        return getInteger(response);
    }

    public Set<String> sMembers(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("SMEMBERS", key);
        return new HashSet<>(Arrays.asList(getStringArray(response)));
    }

    public boolean sIsMember(String key, String member) throws InterruptedException {
        RedisMessage response = client.sendCommand("SISMEMBER", key, member);
        return getInteger(response) == 1;
    }

    public long sCard(String key) throws InterruptedException {
        RedisMessage response = client.sendCommand("SCARD", key);
        return getInteger(response);
    }

    // ==================== 服务器命令 ====================

    public void save() throws InterruptedException {
        client.sendCommand("SAVE");
    }

    public void bgSave() throws InterruptedException {
        client.sendCommand("BGSAVE");
    }

    public long lastSave() throws InterruptedException {
        RedisMessage response = client.sendCommand("LASTSAVE");
        return getInteger(response);
    }

    public String info(String section) throws InterruptedException {
        RedisMessage response = client.sendCommand("INFO", section);
        return getString(response);
    }

    public ScanResult scan(String cursor, String... options) throws InterruptedException {
        return scanCommand("SCAN", null, cursor, options);
    }

    public ScanResult sScan(String key, String cursor, String... options) throws InterruptedException {
        return scanCommand("SSCAN", key, cursor, options);
    }

    public ScanResult hScan(String key, String cursor, String... options) throws InterruptedException {
        return scanCommand("HSCAN", key, cursor, options);
    }

    private ScanResult scanCommand(String commandName, String key, String cursor, String[] options)
            throws InterruptedException {
        int prefixLength = key == null ? 2 : 3;
        String[] command = new String[prefixLength + options.length];
        command[0] = commandName;
        int index = 1;
        if (key != null) command[index++] = key;
        command[index++] = cursor;
        System.arraycopy(options, 0, command, index, options.length);
        RedisMessage response = client.sendCommand(command);
        List<RedisMessage> outer = RedisMessageHelper.extractArray(response);
        if (outer == null || outer.size() != 2) throw new IllegalStateException("invalid SCAN response");
        String nextCursor = getString(outer.get(0));
        List<RedisMessage> values = RedisMessageHelper.extractArray(outer.get(1));
        String[] result = values == null ? new String[0] : values.stream()
                .map(this::getString)
                .toArray(String[]::new);
        return new ScanResult(nextCursor, result);
    }

    public record ScanResult(String cursor, String[] values) {
        public ScanResult {
            values = values.clone();
        }

        @Override
        public String[] values() {
            return values.clone();
        }
    }

    // ==================== 辅助方法 ====================

    private String getString(RedisMessage response) {
        return RedisMessageHelper.extractString(response);
    }

    private long getInteger(RedisMessage response) {
        return RedisMessageHelper.extractLong(response);
    }

    private String[] getStringArray(RedisMessage response) {
        List<RedisMessage> array = RedisMessageHelper.extractArray(response);
        if (array == null || array.isEmpty()) {
            return new String[0];
        }
        String[] result = new String[array.size()];
        for (int i = 0; i < array.size(); i++) {
            result[i] = getString(array.get(i));
        }
        return result;
    }
}
