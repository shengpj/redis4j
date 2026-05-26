package com.redis4j.storage;

import com.redis4j.storage.type.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * 内存数据存储实现
 * 基于 ConcurrentHashMap 实现线程安全的内存存储
 */
public class MemoryStore implements DataStore {

    private final ConcurrentHashMap<String, Entry> store;
    private final ConcurrentHashMap<String, Long> expiryIndex;
    private final ScheduledExecutorService scheduler;

    public MemoryStore() {
        this.store = new ConcurrentHashMap<>();
        this.expiryIndex = new ConcurrentHashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "expiry-cleaner");
            t.setDaemon(true);
            return t;
        });

        // 启动过期 key 清理任务，每秒检查一次
        scheduler.scheduleAtFixedRate(this::cleanupExpiredKeys, 1, 1, TimeUnit.SECONDS);
    }

    // ==================== String 操作 ====================

    @Override
    public void set(String key, String value) {
        store.put(key, new Entry(new RedisString(value)));
    }

    @Override
    public void setEx(String key, String value, long seconds) {
        long expireAt = System.currentTimeMillis() + seconds * 1000;
        RedisString redisValue = new RedisString(value);
        store.put(key, new Entry(redisValue, expireAt));
        expiryIndex.put(key, expireAt);
    }

    @Override
    public boolean setNx(String key, String value) {
        RedisString redisValue = new RedisString(value);
        Entry entry = new Entry(redisValue);
        Entry existing = store.putIfAbsent(key, entry);
        return existing == null;
    }

    @Override
    public String get(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            store.remove(key, entry);
            expiryIndex.remove(key);
            return null;
        }
        RedisValue value = entry.getValue();
        if (value instanceof RedisString) {
            return ((RedisString) value).getStringValue();
        }
        return null;
    }

    @Override
    public String[] mGet(String... keys) {
        String[] results = new String[keys.length];
        for (int i = 0; i < keys.length; i++) {
            results[i] = get(keys[i]);
        }
        return results;
    }

    @Override
    public void mSet(Map<String, String> keyValues) {
        keyValues.forEach(this::set);
    }

    @Override
    public long incr(String key) {
        return incrBy(key, 1);
    }

    @Override
    public long incrBy(String key, long delta) {
        Entry entry = store.compute(key, (k, existing) -> {
            if (existing == null || existing.isExpired()) {
                RedisString newValue = new RedisString(String.valueOf(delta));
                return new Entry(newValue);
            }
            RedisValue value = existing.getValue();
            if (value instanceof RedisString) {
                long current = Long.parseLong(((RedisString) value).getStringValue());
                RedisString newValue = new RedisString(String.valueOf(current + delta));
                return new Entry(newValue, existing.getExpireAt());
            }
            throw new IllegalStateException("Value is not a string");
        });
        expiryIndex.remove(key, entry.getExpireAt());
        return Long.parseLong(((RedisString) store.get(key).getValue()).getStringValue());
    }

    @Override
    public long decr(String key) {
        return incrBy(key, -1);
    }

    @Override
    public long decrBy(String key, long delta) {
        return incrBy(key, -delta);
    }

    @Override
    public long strlen(String key) {
        String value = get(key);
        return value == null ? 0 : value.length();
    }

    @Override
    public long append(String key, String value) {
        Entry entry = store.compute(key, (k, existing) -> {
            if (existing == null || existing.isExpired()) {
                RedisString newValue = new RedisString(value);
                return new Entry(newValue);
            }
            RedisValue rv = existing.getValue();
            if (rv instanceof RedisString) {
                String newStr = ((RedisString) rv).getStringValue() + value;
                RedisString newValue = new RedisString(newStr);
                return new Entry(newValue, existing.getExpireAt());
            }
            throw new IllegalStateException("Value is not a string");
        });
        expiryIndex.remove(key, entry.getExpireAt());
        RedisString rv = (RedisString) store.get(key).getValue();
        return rv.getStringValue().length();
    }

    // ==================== Key 操作 ====================

    @Override
    public long del(String... keys) {
        long count = 0;
        for (String key : keys) {
            if (store.remove(key) != null) {
                count++;
                expiryIndex.remove(key);
            }
        }
        return count;
    }

    @Override
    public boolean exists(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            store.remove(key, entry);
            expiryIndex.remove(key);
            return false;
        }
        return true;
    }

    @Override
    public long exists(String... keys) {
        long count = 0;
        for (String key : keys) {
            if (exists(key)) {
                count++;
            }
        }
        return count;
    }

    @Override
    public boolean expire(String key, long seconds) {
        return expireMs(key, seconds * 1000);
    }

    @Override
    public boolean expireMs(String key, long milliseconds) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            return false;
        }
        long expireAt = System.currentTimeMillis() + milliseconds;
        store.put(key, new Entry(entry.getValue(), expireAt));
        expiryIndex.put(key, expireAt);
        return true;
    }

    @Override
    public long ttl(String key) {
        return pttl(key) / 1000;
    }

    @Override
    public long pttl(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            store.remove(key, entry);
            expiryIndex.remove(key);
            return -2;
        }
        long remaining = entry.getExpireAt() - System.currentTimeMillis();
        if (remaining <= 0) {
            store.remove(key);
            expiryIndex.remove(key);
            return -2;
        }
        return remaining;
    }

    @Override
    public boolean persist(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            return false;
        }
        if (!entry.isPersistent()) {
            store.put(key, new Entry(entry.getValue()));
            expiryIndex.remove(key);
        }
        return true;
    }

    @Override
    public void rename(String key, String newKey) {
        Entry entry = store.remove(key);
        if (entry != null) {
            store.put(newKey, entry);
            if (entry.isPersistent()) {
                expiryIndex.remove(key);
                expiryIndex.put(newKey, entry.getExpireAt());
            }
        }
    }

    @Override
    public DataType type(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            return DataType.NONE;
        }
        return entry.getValue().getType();
    }

    @Override
    public Set<String> keys(String pattern) {
        Set<String> result = new HashSet<>();
        String regex = patternToRegex(pattern);
        for (String key : store.keySet()) {
            if (key.matches(regex)) {
                result.add(key);
            }
        }
        return result;
    }

    @Override
    public long dbSize() {
        cleanupExpiredKeys();
        return store.size();
    }

    // ==================== List 操作 ====================

    @Override
    public long lPush(String key, String... values) {
        return modifyList(key, list -> {
            list.lPush(values);
            return list.size();
        });
    }

    @Override
    public long rPush(String key, String... values) {
        return modifyList(key, list -> {
            list.rPush(values);
            return list.size();
        });
    }

    @Override
    public String lPop(String key) {
        return modifyList(key, RedisList::lPop);
    }

    @Override
    public String rPop(String key) {
        return modifyList(key, RedisList::rPop);
    }

    @Override
    public long lLen(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisList)) {
            return 0;
        }
        return ((RedisList) entry.getValue()).size();
    }

    @Override
    public String[] lRange(String key, long start, long stop) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisList)) {
            return new String[0];
        }
        RedisList list = (RedisList) entry.getValue();
        long size = list.size();
        
        // 处理负数索引
        if (start < 0) {
            start = size + start;
        }
        if (stop < 0) {
            stop = size + stop;
        }
        
        // 调整边界
        if (start < 0) start = 0;
        if (stop >= size) stop = size - 1;
        if (start > stop || start >= size) {
            return new String[0];
        }
        
        int length = (int) (stop - start + 1);
        String[] result = new String[length];
        for (int i = 0; i < length; i++) {
            result[i] = list.get(start + i);
        }
        return result;
    }

    @Override
    public void lSet(String key, long index, String value) {
        modifyList(key, list -> {
            list.set(index, value);
            return null;
        });
    }

    @Override
    public void lTrim(String key, long start, long stop) {
        modifyList(key, list -> {
            list.trim(start, stop);
            return null;
        });
    }

    @Override
    public String lIndex(String key, long index) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisList)) {
            return null;
        }
        return ((RedisList) entry.getValue()).get(index);
    }

    // ==================== Hash 操作 ====================

    @Override
    public long hSet(String key, String field, String value) {
        return modifyHash(key, hash -> {
            long existed = hash.containsKey(field) ? 1 : 0;
            hash.put(field, value);
            return existed;
        });
    }

    @Override
    public boolean hSetNx(String key, String field, String value) {
        return modifyHash(key, hash -> {
            if (hash.containsKey(field)) {
                return false;
            }
            hash.put(field, value);
            return true;
        });
    }

    @Override
    public String hGet(String key, String field) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisHash)) {
            return null;
        }
        return ((RedisHash) entry.getValue()).get(field);
    }

    @Override
    public Map<String, String> hGetAll(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisHash)) {
            return Collections.emptyMap();
        }
        return new HashMap<>(((RedisHash) entry.getValue()).getHash());
    }

    @Override
    public long hDel(String key, String... fields) {
        return modifyHash(key, hash -> {
            long count = 0;
            for (String field : fields) {
                if (hash.remove(field) != null) {
                    count++;
                }
            }
            return count;
        });
    }

    @Override
    public boolean hExists(String key, String field) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisHash)) {
            return false;
        }
        return ((RedisHash) entry.getValue()).containsKey(field);
    }

    @Override
    public long hLen(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisHash)) {
            return 0;
        }
        return ((RedisHash) entry.getValue()).size();
    }

    @Override
    public Set<String> hKeys(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisHash)) {
            return Collections.emptySet();
        }
        return new HashSet<>(((RedisHash) entry.getValue()).keys());
    }

    @Override
    public String[] hVals(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisHash)) {
            return new String[0];
        }
        Collection<String> vals = ((RedisHash) entry.getValue()).values();
        return vals.toArray(new String[0]);
    }

    @Override
    public long hMSet(String key, Map<String, String> fieldValues) {
        return modifyHash(key, hash -> {
            hash.getHash().putAll(fieldValues);
            return (long) fieldValues.size();
        });
    }

    @Override
    public String[] hMGet(String key, String... fields) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisHash)) {
            String[] result = new String[fields.length];
            Arrays.fill(result, null);
            return result;
        }
        RedisHash hash = (RedisHash) entry.getValue();
        String[] result = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            result[i] = hash.get(fields[i]);
        }
        return result;
    }

    @Override
    public long hIncrBy(String key, String field, long delta) {
        return modifyHash(key, hash -> {
            long current = 0;
            String existing = hash.get(field);
            if (existing != null) {
                current = Long.parseLong(existing);
            }
            long newValue = current + delta;
            hash.put(field, String.valueOf(newValue));
            return newValue;
        });
    }

    // ==================== Set 操作 ====================

    @Override
    public long sAdd(String key, String... members) {
        return modifySet(key, set -> {
            long before = set.size();
            set.add(members);
            return set.size() - before;
        });
    }

    @Override
    public long sRem(String key, String... members) {
        return modifySet(key, set -> {
            long before = set.size();
            set.remove(members);
            return before - set.size();
        });
    }

    @Override
    public Set<String> sMembers(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
            return Collections.emptySet();
        }
        return new HashSet<>((Set<String>) entry.getValue().getValue());
    }

    @Override
    public boolean sIsMember(String key, String member) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
            return false;
        }
        return ((RedisSet) entry.getValue()).contains(member);
    }

    @Override
    public long sCard(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
            return 0;
        }
        return ((RedisSet) entry.getValue()).size();
    }

    @Override
    public Set<String> sInter(String... keys) {
        Set<String>[] sets = new Set[keys.length];
        for (int i = 0; i < keys.length; i++) {
            Entry entry = store.get(keys[i]);
            if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
                sets[i] = Collections.emptySet();
            } else {
                sets[i] = (Set<String>) entry.getValue().getValue();
            }
        }
        return RedisSet.inter(sets);
    }

    @Override
    public Set<String> sUnion(String... keys) {
        Set<String>[] sets = new Set[keys.length];
        for (int i = 0; i < keys.length; i++) {
            Entry entry = store.get(keys[i]);
            if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
                sets[i] = Collections.emptySet();
            } else {
                sets[i] = (Set<String>) entry.getValue().getValue();
            }
        }
        return RedisSet.union(sets);
    }

    @Override
    public Set<String> sDiff(String... keys) {
        Set<String>[] sets = new Set[keys.length];
        for (int i = 0; i < keys.length; i++) {
            Entry entry = store.get(keys[i]);
            if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
                sets[i] = Collections.emptySet();
            } else {
                sets[i] = (Set<String>) entry.getValue().getValue();
            }
        }
        return RedisSet.diff(sets);
    }

    @Override
    public boolean sMove(String srcKey, String destKey, String member) {
        Entry srcEntry = store.get(srcKey);
        if (srcEntry == null || srcEntry.isExpired() || !(srcEntry.getValue() instanceof RedisSet)) {
            return false;
        }

        RedisSet srcSet = (RedisSet) srcEntry.getValue();
        if (!srcSet.contains(member)) {
            return false;
        }

        srcSet.remove(member);
        if (srcSet.isEmpty()) {
            store.remove(srcKey);
        }

        Entry destEntry = store.get(destKey);
        RedisSet destSet;
        if (destEntry == null || destEntry.isExpired()) {
            destSet = new RedisSet();
            long expireAt = destEntry != null ? destEntry.getExpireAt() : -1;
            store.put(destKey, new Entry(destSet, expireAt));
        } else if (destEntry.getValue() instanceof RedisSet) {
            destSet = (RedisSet) destEntry.getValue();
        } else {
            return false;
        }

        destSet.add(member);
        return true;
    }

    @Override
    public String sPop(String key) {
        return modifySet(key, set -> {
            if (set.isEmpty()) {
                return null;
            }
            Iterator<String> iterator = set.iterator();
            String member = iterator.next();
            iterator.remove();
            return member;
        });
    }

    @Override
    public String sRandMember(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
            return null;
        }
        return ((RedisSet) entry.getValue()).randomMember();
    }

    @Override
    public String[] sRandMember(String key, long count) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
            return new String[0];
        }
        List<String> members = ((RedisSet) entry.getValue()).randomMembers(count);
        return members.toArray(new String[0]);
    }

    // ==================== 服务器操作 ====================

    @Override
    public void flushDb() {
        store.clear();
        expiryIndex.clear();
    }

    @Override
    public void flushAll() {
        flushDb();
    }

    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        store.clear();
        expiryIndex.clear();
    }

    // ==================== 内部方法 ====================

    /**
     * 清理过期 key
     */
    private void cleanupExpiredKeys() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : expiryIndex.entrySet()) {
            if (entry.getValue() < now) {
                store.remove(entry.getKey());
                expiryIndex.remove(entry.getKey());
            }
        }
    }

    /**
     * 通用 List 修改方法
     */
    private <T> T modifyList(String key, java.util.function.Function<RedisList, T> operation) {
        while (true) {
            Entry entry = store.get(key);
            if (entry == null || entry.isExpired()) {
                RedisList list = new RedisList();
                Entry newEntry = new Entry(list);
                if (store.putIfAbsent(key, newEntry) == null) {
                    T result = operation.apply(list);
                    if (list.isEmpty()) {
                        store.remove(key);
                    }
                    return result;
                }
                continue;
            }

            if (!(entry.getValue() instanceof RedisList)) {
                throw new IllegalStateException("Value is not a list");
            }

            RedisList list = (RedisList) entry.getValue();
            T result = operation.apply(list);

            if (list.isEmpty()) {
                store.remove(key);
            }

            return result;
        }
    }

    /**
     * 通用 Hash 修改方法
     */
    private <T> T modifyHash(String key, java.util.function.Function<RedisHash, T> operation) {
        while (true) {
            Entry entry = store.get(key);
            if (entry == null || entry.isExpired()) {
                RedisHash hash = new RedisHash();
                Entry newEntry = new Entry(hash);
                if (store.putIfAbsent(key, newEntry) == null) {
                    T result = operation.apply(hash);
                    if (hash.isEmpty()) {
                        store.remove(key);
                    }
                    return result;
                }
                continue;
            }

            if (!(entry.getValue() instanceof RedisHash)) {
                throw new IllegalStateException("Value is not a hash");
            }

            RedisHash hash = (RedisHash) entry.getValue();
            T result = operation.apply(hash);

            if (hash.isEmpty()) {
                store.remove(key);
            }

            return result;
        }
    }

    /**
     * 通用 Set 修改方法
     */
    private <T> T modifySet(String key, java.util.function.Function<RedisSet, T> operation) {
        while (true) {
            Entry entry = store.get(key);
            if (entry == null || entry.isExpired()) {
                RedisSet set = new RedisSet();
                Entry newEntry = new Entry(set);
                if (store.putIfAbsent(key, newEntry) == null) {
                    T result = operation.apply(set);
                    if (set.isEmpty()) {
                        store.remove(key);
                    }
                    return result;
                }
                continue;
            }

            if (!(entry.getValue() instanceof RedisSet)) {
                throw new IllegalStateException("Value is not a set");
            }

            RedisSet set = (RedisSet) entry.getValue();
            T result = operation.apply(set);

            if (set.isEmpty()) {
                store.remove(key);
            }

            return result;
        }
    }

    /**
     * 将 pattern 转换为正则表达式
     */
    private String patternToRegex(String pattern) {
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append(".");
                case '.' -> regex.append("\\.");
                case '(' -> regex.append("\\(");
                case ')' -> regex.append("\\)");
                case '+' -> regex.append("\\+");
                case '[' -> regex.append("\\[");
                case ']' -> regex.append("\\]");
                default -> regex.append(c);
            }
        }
        regex.append("$");
        return regex.toString();
    }
}
