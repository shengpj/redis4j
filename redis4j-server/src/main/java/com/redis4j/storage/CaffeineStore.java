package com.redis4j.storage;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.redis4j.storage.type.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * 基于 Caffeine 的数据存储实现
 *
 * 用 Caffeine Cache 作为底层存储，设定 maximumSize 并在驱逐时通过监听器输出 warn 日志。
 * 不支持 TTL（ttl/pttl 始终返回 -1，expire/setEx 退化为无过期存储）。
 * 相比 MemoryStore 省掉了 expiryIndex 和定时清理线程。
 */
public class CaffeineStore implements DataStore {

    private static final Logger logger = LoggerFactory.getLogger(CaffeineStore.class);

    private final Cache<String, Entry> cache;
    private final long maxSize;

    public CaffeineStore(long maxSize) {
        this.maxSize = maxSize;
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .evictionListener((String key, Entry entry, RemovalCause cause) ->
                        logger.warn("Evicted key '{}' (type={}, cause={}, maxSize={})",
                                key, entry != null ? entry.getValue().getType() : "null", cause, maxSize))
                .build();
        logger.info("CaffeineStore created with maxSize={}", maxSize);
    }

    private ConcurrentMap<String, Entry> map() {
        return cache.asMap();
    }

    // ==================== String 操作 ====================

    @Override
    public void set(String key, String value) {
        map().put(key, new Entry(new RedisString(value)));
        checkSize();
    }

    @Override
    public void setEx(String key, String value, long seconds) {
        set(key, value); // 简化：忽略 TTL
    }

    @Override
    public boolean setNx(String key, String value) {
        Entry existing = map().putIfAbsent(key, new Entry(new RedisString(value)));
        boolean ok = existing == null;
        if (ok) checkSize();
        return ok;
    }

    @Override
    public String get(String key) {
        Entry entry = map().get(key);
        if (entry == null) return null;
        if (entry.getValue() instanceof RedisString s) {
            return s.getStringValue();
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
        Entry entry = map().compute(key, (k, existing) -> {
            if (existing == null) {
                return new Entry(new RedisString(String.valueOf(delta)));
            }
            if (existing.getValue() instanceof RedisString s) {
                long current = Long.parseLong(s.getStringValue());
                return new Entry(new RedisString(String.valueOf(current + delta)));
            }
            throw new IllegalStateException("WRONGTYPE Value is not a string");
        });
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
        Entry entry = map().compute(key, (k, existing) -> {
            if (existing == null) {
                return new Entry(new RedisString(value));
            }
            if (existing.getValue() instanceof RedisString s) {
                return new Entry(new RedisString(s.getStringValue() + value));
            }
            throw new IllegalStateException("WRONGTYPE Value is not a string");
        });
        return ((RedisString) entry.getValue()).getStringValue().length();
    }

    // ==================== Key 操作 ====================

    @Override
    public long del(String... keys) {
        long count = 0;
        for (String key : keys) {
            if (map().remove(key) != null) count++;
        }
        return count;
    }

    @Override
    public boolean exists(String key) {
        return map().get(key) != null;
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
        return false; // 不支持 TTL
    }

    @Override
    public boolean expireMs(String key, long milliseconds) {
        return false;
    }

    @Override
    public long ttl(String key) {
        return entryExists(key) ? -1 : -2;
    }

    @Override
    public long pttl(String key) {
        return entryExists(key) ? -1 : -2;
    }

    @Override
    public boolean persist(String key) {
        return entryExists(key);
    }

    @Override
    public void rename(String key, String newKey) {
        if (key.equals(newKey)) return;
        map().compute(key, (k, entry) -> {
            if (entry == null) {
                return null;
            }
            map().put(newKey, entry);
            return null;
        });
    }

    @Override
    public DataType type(String key) {
        Entry entry = map().get(key);
        if (entry == null) return DataType.NONE;
        return entry.getValue().getType();
    }

    @Override
    public Set<String> keys(String pattern) {
        Set<String> result = new HashSet<>();
        String regex = patternToRegex(pattern);
        for (String key : map().keySet()) {
            if (key.matches(regex)) {
                result.add(key);
            }
        }
        return result;
    }

    @Override
    public long dbSize() {
        return cache.estimatedSize();
    }

    // ==================== List 操作 ====================

    @Override
    public long lPush(String key, String... values) {
        return modifyList(key, list -> { list.lPush(values); return list.size(); });
    }

    @Override
    public long rPush(String key, String... values) {
        return modifyList(key, list -> { list.rPush(values); return list.size(); });
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
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisList list)) return 0;
        return list.size();
    }

    @Override
    public String[] lRange(String key, long start, long stop) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisList list)) return new String[0];

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
        modifyList(key, list -> { list.set(index, value); return null; });
    }

    @Override
    public void lTrim(String key, long start, long stop) {
        modifyList(key, list -> { list.trim(start, stop); return null; });
    }

    @Override
    public String lIndex(String key, long index) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisList list)) return null;
        return list.get(index);
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
            if (hash.containsKey(field)) return false;
            hash.put(field, value);
            return true;
        });
    }

    @Override
    public String hGet(String key, String field) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisHash hash)) return null;
        return hash.get(field);
    }

    @Override
    public Map<String, String> hGetAll(String key) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisHash hash)) return Collections.emptyMap();
        return new HashMap<>(hash.getHash());
    }

    @Override
    public long hDel(String key, String... fields) {
        return modifyHash(key, hash -> {
            long count = 0;
            for (String f : fields) {
                if (hash.remove(f) != null) count++;
            }
            return count;
        });
    }

    @Override
    public boolean hExists(String key, String field) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisHash hash)) return false;
        return hash.containsKey(field);
    }

    @Override
    public long hLen(String key) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisHash hash)) return 0;
        return hash.size();
    }

    @Override
    public Set<String> hKeys(String key) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisHash hash)) return Collections.emptySet();
        return new HashSet<>(hash.keys());
    }

    @Override
    public String[] hVals(String key) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisHash hash)) return new String[0];
        return hash.values().toArray(new String[0]);
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
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisHash hash)) {
            String[] result = new String[fields.length];
            Arrays.fill(result, null);
            return result;
        }
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
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisSet set)) return Collections.emptySet();
        return new HashSet<>(set.getSet());
    }

    @Override
    public boolean sIsMember(String key, String member) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisSet set)) return false;
        return set.contains(member);
    }

    @Override
    public long sCard(String key) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisSet set)) return 0;
        return set.size();
    }

    @Override
    public Set<String> sInter(String... keys) {
        Set<String>[] sets = Arrays.stream(keys).map(k -> {
            Entry entry = map().get(k);
            if (entry == null || !(entry.getValue() instanceof RedisSet set)) return Collections.<String>emptySet();
            return (Set<String>) set.getSet();
        }).toArray(Set[]::new);
        return RedisSet.inter(sets);
    }

    @Override
    public Set<String> sUnion(String... keys) {
        Set<String>[] sets = Arrays.stream(keys).map(k -> {
            Entry entry = map().get(k);
            if (entry == null || !(entry.getValue() instanceof RedisSet set)) return Collections.<String>emptySet();
            return (Set<String>) set.getSet();
        }).toArray(Set[]::new);
        return RedisSet.union(sets);
    }

    @Override
    public Set<String> sDiff(String... keys) {
        Set<String>[] sets = Arrays.stream(keys).map(k -> {
            Entry entry = map().get(k);
            if (entry == null || !(entry.getValue() instanceof RedisSet set)) return Collections.<String>emptySet();
            return (Set<String>) set.getSet();
        }).toArray(Set[]::new);
        return RedisSet.diff(sets);
    }

    @Override
    public boolean sMove(String srcKey, String destKey, String member) {
        final boolean[] result = {false};
        map().computeIfPresent(srcKey, (k, entry) -> {
            if (entry == null || !(entry.getValue() instanceof RedisSet srcSet)) {
                return entry;
            }
            if (!srcSet.contains(member)) {
                return entry;
            }

            // 在 computeIfPresent 锁内完成目标写入
            Entry destEntry = map().get(destKey);
            RedisSet destSet;
            if (destEntry == null) {
                destSet = new RedisSet();
                map().put(destKey, new Entry(destSet));
            } else if (destEntry.getValue() instanceof RedisSet s) {
                destSet = new RedisSet(s.getSet());
            } else {
                return entry; // 类型不匹配，不做任何修改
            }
            destSet.add(member);

            result[0] = true;
            // 从源创建去重的新集合
            Set<String> newSrc = new HashSet<>(srcSet.getSet());
            newSrc.remove(member);
            if (newSrc.isEmpty()) {
                return null; // 集合为空，删除源 key
            }
            return new Entry(new RedisSet(newSrc));
        });
        return result[0];
    }

    @Override
    public String sPop(String key) {
        return modifySet(key, set -> {
            if (set.isEmpty()) return null;
            Iterator<String> it = set.iterator();
            String member = it.next();
            it.remove();
            return member;
        });
    }

    @Override
    public String sRandMember(String key) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisSet set)) return null;
        return set.randomMember();
    }

    @Override
    public String[] sRandMember(String key, long count) {
        Entry entry = map().get(key);
        if (entry == null || !(entry.getValue() instanceof RedisSet set)) return new String[0];
        return set.randomMembers(count).toArray(new String[0]);
    }

    // ==================== 服务器操作 ====================

    @Override
    public void flushDb() {
        cache.invalidateAll();
    }

    @Override
    public void flushAll() {
        flushDb();
    }

    @Override
    public void close() {
        cache.cleanUp();
    }

    @Override
    public Set<String> getAllKeys() {
        return new HashSet<>(map().keySet());
    }

    // ==================== 内部方法 ====================

    private boolean entryExists(String key) {
        return map().get(key) != null;
    }

    /**
     * 写入后检查缓存大小，超过上限时输出警告
     */
    private void checkSize() {
        long size = cache.estimatedSize();
        if (size > maxSize) {
            logger.warn("Cache size {} exceeds maxSize {}, further keys will be evicted", size, maxSize);
        }
    }

    // ==================== 泛型修改方法 ====================

    private <T> T modifyList(String key, Function<RedisList, T> op) {
        while (true) {
            Entry entry = map().get(key);
            if (entry == null) {
                RedisList list = new RedisList();
                if (map().putIfAbsent(key, new Entry(list)) == null) {
                    T result = op.apply(list);
                    if (list.isEmpty()) map().remove(key);
                    return result;
                }
                continue;
            }
            if (!(entry.getValue() instanceof RedisList list)) {
                throw new IllegalStateException("WRONGTYPE Key is not a list");
            }
            T result = op.apply(list);
            if (list.isEmpty()) map().remove(key);
            return result;
        }
    }

    private <T> T modifyHash(String key, Function<RedisHash, T> op) {
        while (true) {
            Entry entry = map().get(key);
            if (entry == null) {
                RedisHash hash = new RedisHash();
                if (map().putIfAbsent(key, new Entry(hash)) == null) {
                    T result = op.apply(hash);
                    if (hash.isEmpty()) map().remove(key);
                    return result;
                }
                continue;
            }
            if (!(entry.getValue() instanceof RedisHash hash)) {
                throw new IllegalStateException("WRONGTYPE Key is not a hash");
            }
            T result = op.apply(hash);
            if (hash.isEmpty()) map().remove(key);
            return result;
        }
    }

    private <T> T modifySet(String key, Function<RedisSet, T> op) {
        while (true) {
            Entry entry = map().get(key);
            if (entry == null) {
                RedisSet set = new RedisSet();
                if (map().putIfAbsent(key, new Entry(set)) == null) {
                    T result = op.apply(set);
                    if (set.isEmpty()) map().remove(key);
                    return result;
                }
                continue;
            }
            if (!(entry.getValue() instanceof RedisSet set)) {
                throw new IllegalStateException("WRONGTYPE Key is not a set");
            }
            T result = op.apply(set);
            if (set.isEmpty()) map().remove(key);
            return result;
        }
    }

    // ==================== Pattern 转换 ====================

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
