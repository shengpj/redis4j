package com.redis4j.storage;

import com.redis4j.storage.type.*;
import com.redis4j.storage.expiration.ExpirationPolicy;
import com.redis4j.storage.expiration.IndexedExpirationPolicy;
import com.redis4j.storage.repository.ConcurrentMapEntryRepository;
import com.redis4j.storage.repository.EntryRepository;

import java.util.*;
import java.util.concurrent.*;

/**
 * 内存数据存储实现
 * 基于 ConcurrentHashMap 实现线程安全的内存存储
 */
public class MemoryStore implements DataStore {

    private final EntryRepository store;
    private final ExpirationPolicy expiryIndex;
    private final ScheduledExecutorService scheduler;

    public MemoryStore() {
        this.store = new ConcurrentMapEntryRepository();
        this.expiryIndex = new IndexedExpirationPolicy();
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
                return new Entry(new RedisString(String.valueOf(delta)));
            }
            RedisValue value = existing.getValue();
            if (value instanceof RedisString) {
                long current = Long.parseLong(((RedisString) value).getStringValue());
                return new Entry(new RedisString(String.valueOf(current + delta)), existing.getExpireAt());
            }
            throw new IllegalStateException("WRONGTYPE Value is not a string");
        });
        // 如果新 entry 无过期时间，清理可能的过期索引（之前在 expired key 上操作时残留）
        if (entry.isPersistent()) {
            expiryIndex.remove(key);
        }
        return Long.parseLong(((RedisString) entry.getValue()).getStringValue());
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
                return new Entry(new RedisString(value));
            }
            RedisValue rv = existing.getValue();
            if (rv instanceof RedisString) {
                String newStr = ((RedisString) rv).getStringValue() + value;
                return new Entry(new RedisString(newStr), existing.getExpireAt());
            }
            throw new IllegalStateException("WRONGTYPE Value is not a string");
        });
        if (entry.isPersistent()) {
            expiryIndex.remove(key);
        }
        return ((RedisString) entry.getValue()).getStringValue().length();
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
        long result = pttl(key);
        return result >= 0 ? result / 1000 : result;
    }

    @Override
    public long pttl(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            store.remove(key, entry);
            expiryIndex.remove(key);
            return -2;
        }
        if (entry.isPersistent()) {
            return -1;
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
        store.compute(key, (k, entry) -> {
            if (entry == null) {
                return null; // 源 key 不存在，不做任何操作
            }
            if (k.equals(newKey)) {
                return entry; // 同源同 key，不操作
            }
            // 原子地将 entry 从 key 迁移到 newKey
            store.put(newKey, entry);
            expiryIndex.remove(k);
            if (!entry.isPersistent()) {
                expiryIndex.put(newKey, entry.getExpireAt());
            }
            return null; // 删除源 key
        });
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
        final boolean[] result = {false};
        store.computeIfPresent(srcKey, (k, entry) -> {
            if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet srcSet)) {
                return entry;
            }
            if (!srcSet.contains(member)) {
                return entry;
            }

            // 在返回新 entry 前，先向目标写入 member（同 computeIfPresent 锁内）
            Entry destEntry = store.get(destKey);
            if (destEntry != null && destEntry.isExpired()) {
                destEntry = null;
            }
            RedisSet destSet;
            if (destEntry == null) {
                destSet = new RedisSet();
                store.put(destKey, new Entry(destSet));
            } else if (destEntry.getValue() instanceof RedisSet s) {
                destSet = new RedisSet(s.getSet());
            } else {
                return entry; // 类型不匹配，不做任何修改
            }
            destSet.add(member);

            result[0] = true;
            expiryIndex.remove(k);
            // 从源创建去重的新集合
            Set<String> newSrc = new HashSet<>(srcSet.getSet());
            newSrc.remove(member);
            if (newSrc.isEmpty()) {
                return null; // 集合为空，删除源 key
            }
            return new Entry(new RedisSet(newSrc), entry.getExpireAt());
        });
        return result[0];
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

    @Override
    public Set<String> getAllKeys() {
        return new HashSet<>(store.keySet());
    }

    // ==================== 内部方法 ====================

    /**
     * 清理过期 key
     */
    private void cleanupExpiredKeys() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : expiryIndex.entrySet()) {
            Long expireAt = entry.getValue();
            if (expireAt != null && expireAt < now) {
                // 原子删除：只有过期时间未变时才清理，避免误删被刷新过过期时间的 key
                String key = entry.getKey();
                if (expiryIndex.remove(key, expireAt)) {
                    store.remove(key);
                }
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
