package com.redis4j.storage;

import com.redis4j.storage.type.*;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;

import java.util.*;
import java.util.concurrent.*;

/**
 * 内存数据存储实现 - 基于 Eclipse Collections
 * 使用 Eclipse Collections 的高效数据结构
 * 
 * @deprecated 性能不如 MemoryStore，已被废弃
 */
@Deprecated
public class EclipseCollectionsStore implements DataStore {

    private final MutableMap<String, Entry> store;
    private final MutableMap<String, Long> expiryIndex;
    private final ScheduledExecutorService scheduler;

    public EclipseCollectionsStore() {
        this.store = ConcurrentHashMap.newMap();
        this.expiryIndex = ConcurrentHashMap.newMap();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "eclipse-expiry-cleaner");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::cleanupExpiredKeys, 1, 1, TimeUnit.SECONDS);
    }

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
        if (!store.containsKey(key)) {
            store.put(key, entry);
            return true;
        }
        return false;
    }

    @Override
    public String get(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            store.remove(key);
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
        while (true) {
            Entry entry = store.get(key);
            if (entry == null || entry.isExpired()) {
                RedisString newValue = new RedisString(String.valueOf(delta));
                store.put(key, new Entry(newValue));
                return delta;
            }
            RedisValue value = entry.getValue();
            if (value instanceof RedisString) {
                long current = Long.parseLong(((RedisString) value).getStringValue());
                RedisString newValue = new RedisString(String.valueOf(current + delta));
                store.put(key, new Entry(newValue, entry.getExpireAt()));
                return current + delta;
            }
            throw new IllegalStateException("Value is not a string");
        }
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
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisString)) {
            return 0;
        }
        return ((RedisString) entry.getValue()).getStringValue().length();
    }

    @Override
    public long append(String key, String value) {
        while (true) {
            Entry entry = store.get(key);
            if (entry == null || entry.isExpired()) {
                RedisString newValue = new RedisString(value);
                store.put(key, new Entry(newValue));
                return value.length();
            }
            RedisValue rv = entry.getValue();
            if (rv instanceof RedisString) {
                String current = ((RedisString) rv).getStringValue();
                RedisString newValue = new RedisString(current + value);
                store.put(key, new Entry(newValue, entry.getExpireAt()));
                return (current + value).length();
            }
            throw new IllegalStateException("Value is not a string");
        }
    }

    @Override
    public long del(String... keys) {
        long count = 0;
        for (String key : keys) {
            if (store.remove(key) != null) {
                expiryIndex.remove(key);
                count++;
            }
        }
        return count;
    }

    @Override
    public boolean exists(String key) {
        Entry entry = store.get(key);
        return entry != null && !entry.isExpired();
    }

    @Override
    public long exists(String... keys) {
        long count = 0;
        for (String key : keys) {
            if (exists(key)) count++;
        }
        return count;
    }

    @Override
    public boolean expire(String key, long seconds) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            return false;
        }
        long expireAt = System.currentTimeMillis() + seconds * 1000;
        store.put(key, new Entry(entry.getValue(), expireAt));
        expiryIndex.put(key, expireAt);
        return true;
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
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            store.remove(key);
            expiryIndex.remove(key);
            return -2;
        }
        long remaining = entry.getExpireAt() - System.currentTimeMillis();
        if (remaining <= 0) {
            store.remove(key);
            expiryIndex.remove(key);
            return -2;
        }
        return remaining / 1000;
    }

    @Override
    public long pttl(String key) {
        Entry entry = store.get(key);
        if (entry == null || entry.isExpired()) {
            store.remove(key);
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
        if (key.equals(newKey)) return;
        store.computeIfPresent(key, (k, entry) -> {
            store.put(newKey, entry);
            expiryIndex.remove(k);
            return null;
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
        for (String key : store.keysView().toList()) {
            if (key.matches(regex)) {
                result.add(key);
            }
        }
        return result;
    }

    @Override
    public long dbSize() {
        long count = 0;
        for (Entry entry : store.valuesView()) {
            if (!entry.isExpired()) count++;
        }
        return count;
    }

    @Override
    public long lPush(String key, String... values) {
        return modifyList(key, list -> {
            for (int i = values.length - 1; i >= 0; i--) {
                list.add(0, values[i]);
            }
            return list.size();
        });
    }

    @Override
    public long rPush(String key, String... values) {
        return modifyList(key, list -> {
            for (String value : values) {
                list.add(value);
            }
            return list.size();
        });
    }

    @Override
    public String lPop(String key) {
        return modifyList(key, list -> {
            if (list.isEmpty()) return null;
            return list.remove(0);
        });
    }

    @Override
    public String rPop(String key) {
        return modifyList(key, list -> {
            if (list.isEmpty()) return null;
            return list.remove((int)(list.size() - 1));
        });
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
        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;
        if (start < 0) start = 0;
        if (stop >= size) stop = size - 1;
        if (start > stop || start >= size) return new String[0];
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
            list.set((int) index, value);
            return null;
        });
    }

    @Override
    public void lTrim(String key, long start, long stop) {
        modifyList(key, list -> {
            long size = list.size();
            long normalizedStart = start < 0 ? size + start : start;
            long normalizedStop = stop < 0 ? size + stop : stop;
            normalizedStart = Math.max(0, normalizedStart);
            normalizedStop = Math.min(size - 1, normalizedStop);
            if (normalizedStart > normalizedStop) {
                list.clear();
            } else {
                int stopIdx = (int) normalizedStop;
                int startIdx = (int) normalizedStart;
                // Remove from end first to avoid index shifting issues
                for (int i = (int) list.size() - 1; i > stopIdx; i--) {
                    list.remove(i);
                }
                // Then remove from start
                for (int i = 0; i < startIdx; i++) {
                    list.remove(0);
                }
            }
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

    @Override
    public long hSet(String key, String field, String value) {
        return modifyHash(key, hash -> {
            String old = hash.put(field, value);
            return old == null ? 1L : 0L;
        });
    }

    @Override
    public boolean hSetNx(String key, String field, String value) {
        return modifyHash(key, hash -> {
            if (hash.containsKey(field)) return false;
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
                if (hash.remove(field) != null) count++;
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
            long count = 0;
            for (Map.Entry<String, String> e : fieldValues.entrySet()) {
                if (hash.put(e.getKey(), e.getValue()) == null) count++;
            }
            return count;
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
            if (existing != null) current = Long.parseLong(existing);
            long newValue = current + delta;
            hash.put(field, String.valueOf(newValue));
            return newValue;
        });
    }

    @Override
    public long sAdd(String key, String... members) {
        return modifySet(key, set -> {
            long count = 0;
            for (String member : members) {
                if (set.add(member)) count++;
            }
            return count;
        });
    }

    @Override
    public long sRem(String key, String... members) {
        return modifySet(key, set -> {
            long count = 0;
            for (String member : members) {
                if (set.remove(member)) count++;
            }
            return count;
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

        // 创建去重的新源集合（避免修改存储中的原始对象）
        Set<String> newSrc = new HashSet<>(srcSet.getSet());
        newSrc.remove(member);

        // 先写入目标，失败则不做任何修改
        Entry destEntry = store.get(destKey);
        RedisSet destSet;
        if (destEntry == null || destEntry.isExpired()) {
            destSet = new RedisSet();
            store.put(destKey, new Entry(destSet));
        } else if (destEntry.getValue() instanceof RedisSet s) {
            destSet = new RedisSet(s.getSet());
        } else {
            return false;
        }
        destSet.add(member);

        // 全部写入成功，再更新源
        if (newSrc.isEmpty()) {
            store.remove(srcKey);
        } else {
            store.put(srcKey, new Entry(new RedisSet(newSrc), srcEntry.getExpireAt()));
        }
        return true;
    }

    @Override
    public String sPop(String key) {
        return modifySet(key, RedisSet::pop);
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
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Set<String> getAllKeys() {
        return new HashSet<>(store.keySet());
    }

    private <T> T modifyList(String key, java.util.function.Function<RedisList, T> operation) {
        while (true) {
            Entry entry = store.get(key);
            if (entry == null || entry.isExpired()) {
                RedisList list = new RedisList();
                store.put(key, new Entry(list));
                T result = operation.apply(list);
                if (list.isEmpty()) store.removeKey(key);
                return result;
            }
            if (!(entry.getValue() instanceof RedisList)) {
                throw new IllegalStateException("Value is not a list");
            }
            RedisList list = (RedisList) entry.getValue();
            T result = operation.apply(list);
            if (list.isEmpty()) store.removeKey(key);
            return result;
        }
    }

    private <T> T modifyHash(String key, java.util.function.Function<RedisHash, T> operation) {
        while (true) {
            Entry entry = store.get(key);
            if (entry == null || entry.isExpired()) {
                RedisHash hash = new RedisHash();
                store.put(key, new Entry(hash));
                T result = operation.apply(hash);
                if (hash.isEmpty()) store.removeKey(key);
                return result;
            }
            if (!(entry.getValue() instanceof RedisHash)) {
                throw new IllegalStateException("Value is not a hash");
            }
            RedisHash hash = (RedisHash) entry.getValue();
            T result = operation.apply(hash);
            if (hash.isEmpty()) store.removeKey(key);
            return result;
        }
    }

    private <T> T modifySet(String key, java.util.function.Function<RedisSet, T> operation) {
        while (true) {
            Entry entry = store.get(key);
            if (entry == null || entry.isExpired()) {
                RedisSet set = new RedisSet();
                store.put(key, new Entry(set));
                T result = operation.apply(set);
                if (set.isEmpty()) store.removeKey(key);
                return result;
            }
            if (!(entry.getValue() instanceof RedisSet)) {
                throw new IllegalStateException("Value is not a set");
            }
            RedisSet set = (RedisSet) entry.getValue();
            T result = operation.apply(set);
            if (set.isEmpty()) store.removeKey(key);
            return result;
        }
    }

    private void cleanupExpiredKeys() {
        for (String key : store.keysView().toList()) {
            Entry entry = store.get(key);
            if (entry != null && entry.isExpired()) {
                store.removeKey(key);
                expiryIndex.remove(key);
            }
        }
    }

    private String patternToRegex(String pattern) {
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '*') {
                regex.append(".*");
            } else if (c == '?') {
                regex.append('.');
            } else if (c == '.') {
                regex.append("\\.");
            } else {
                regex.append(c);
            }
        }
        regex.append("$");
        return regex.toString();
    }
}
