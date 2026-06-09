package com.redis4j.storage;

import com.redis4j.storage.type.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 分区内存数据存储实现
 * 基于 MemoryStore，将数据分散到多个分区以减少锁竞争
 *
 * 每个分区有独立的 ConcurrentHashMap，分区选择基于 key 的 hash 值
 */
public class PartitionedMemoryStore implements DataStore {

    private static final int DEFAULT_PARTITIONS;
    private final Partition[] partitions;
    private final int numPartitions;
    private final ScheduledExecutorService scheduler;

    static {
        DEFAULT_PARTITIONS = Runtime.getRuntime().availableProcessors();
    }

    public PartitionedMemoryStore() {
        this(DEFAULT_PARTITIONS);
    }

    public PartitionedMemoryStore(int numPartitions) {
        if (numPartitions < 1) {
            throw new IllegalArgumentException("分区数必须大于0");
        }
        this.numPartitions = numPartitions;
        this.partitions = new Partition[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = new Partition();
        }
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "expiry-cleaner");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::cleanupExpiredKeys, 1, 1, TimeUnit.SECONDS);
    }

    private int getPartitionIndex(String key) {
        return hash(key) % numPartitions;
    }

    private static int hash(String key) {
        int h = key.hashCode();
        return h ^ (h >>> 16);
    }

    private Partition getPartition(String key) {
        return partitions[getPartitionIndex(key)];
    }

    // ==================== String 操作 ====================

    @Override
    public void set(String key, String value) {
        getPartition(key).store.put(key, new Entry(new RedisString(value)));
    }

    @Override
    public void setEx(String key, String value, long seconds) {
        Partition p = getPartition(key);
        long expireAt = System.currentTimeMillis() + seconds * 1000;
        RedisString redisValue = new RedisString(value);
        p.store.put(key, new Entry(redisValue, expireAt));
        p.expiryIndex.put(key, expireAt);
    }

    @Override
    public boolean setNx(String key, String value) {
        Partition p = getPartition(key);
        RedisString redisValue = new RedisString(value);
        Entry entry = new Entry(redisValue);
        Entry existing = p.store.putIfAbsent(key, entry);
        return existing == null;
    }

    @Override
    public String get(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired()) {
            p.store.remove(key, entry);
            p.expiryIndex.remove(key);
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
        Partition p = getPartition(key);
        Entry entry = p.store.compute(key, (k, existing) -> {
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
        p.expiryIndex.remove(key, entry.getExpireAt());
        return Long.parseLong(((RedisString) p.store.get(key).getValue()).getStringValue());
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
        Partition p = getPartition(key);
        Entry entry = p.store.compute(key, (k, existing) -> {
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
        p.expiryIndex.remove(key, entry.getExpireAt());
        RedisString rv = (RedisString) p.store.get(key).getValue();
        return rv.getStringValue().length();
    }

    // ==================== Key 操作 ====================

    @Override
    public long del(String... keys) {
        long count = 0;
        for (String key : keys) {
            Partition p = getPartition(key);
            if (p.store.remove(key) != null) {
                count++;
                p.expiryIndex.remove(key);
            }
        }
        return count;
    }

    @Override
    public boolean exists(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired()) {
            p.store.remove(key, entry);
            p.expiryIndex.remove(key);
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
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired()) {
            return false;
        }
        long expireAt = System.currentTimeMillis() + milliseconds;
        p.store.put(key, new Entry(entry.getValue(), expireAt));
        p.expiryIndex.put(key, expireAt);
        return true;
    }

    @Override
    public long ttl(String key) {
        return pttl(key) / 1000;
    }

    @Override
    public long pttl(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired()) {
            p.store.remove(key, entry);
            p.expiryIndex.remove(key);
            return -2;
        }
        long remaining = entry.getExpireAt() - System.currentTimeMillis();
        if (remaining <= 0) {
            p.store.remove(key);
            p.expiryIndex.remove(key);
            return -2;
        }
        return remaining;
    }

    @Override
    public boolean persist(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired()) {
            return false;
        }
        if (!entry.isPersistent()) {
            p.store.put(key, new Entry(entry.getValue()));
            p.expiryIndex.remove(key);
        }
        return true;
    }

    @Override
    public void rename(String key, String newKey) {
        if (key.equals(newKey)) return;

        Partition p1 = getPartition(key);
        Partition p2 = getPartition(newKey);

        p1.store.compute(key, (k, entry) -> {
            if (entry == null) {
                return null;
            }
            // 同分区：直接移动
            if (p1 == p2) {
                p1.store.put(newKey, entry);
                p1.expiryIndex.remove(k);
                if (!entry.isPersistent()) {
                    p1.expiryIndex.put(newKey, entry.getExpireAt());
                }
                return null;
            }
            // 跨分区：先放目标，再删除源
            p2.store.put(newKey, entry);
            if (entry.isPersistent()) {
                p2.expiryIndex.put(newKey, -1L);
            } else {
                p2.expiryIndex.put(newKey, entry.getExpireAt());
            }
            return null;
        });
    }

    @Override
    public DataType type(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired()) {
            return DataType.NONE;
        }
        return entry.getValue().getType();
    }

    @Override
    public Set<String> keys(String pattern) {
        Set<String> result = new HashSet<>();
        String regex = patternToRegex(pattern);
        for (Partition p : partitions) {
            for (String key : Collections.list(p.store.keys())) {
                if (key.matches(regex)) {
                    result.add(key);
                }
            }
        }
        return result;
    }

    @Override
    public long dbSize() {
        cleanupExpiredKeys();
        long total = 0;
        for (Partition p : partitions) {
            total += p.store.size();
        }
        return total;
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
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisList)) {
            return 0;
        }
        return ((RedisList) entry.getValue()).size();
    }

    @Override
    public String[] lRange(String key, long start, long stop) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisList)) {
            return new String[0];
        }
        RedisList list = (RedisList) entry.getValue();
        long size = list.size();

        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;

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
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
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
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisHash)) {
            return null;
        }
        return ((RedisHash) entry.getValue()).get(field);
    }

    @Override
    public Map<String, String> hGetAll(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
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
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisHash)) {
            return false;
        }
        return ((RedisHash) entry.getValue()).containsKey(field);
    }

    @Override
    public long hLen(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisHash)) {
            return 0;
        }
        return ((RedisHash) entry.getValue()).size();
    }

    @Override
    public Set<String> hKeys(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisHash)) {
            return Collections.emptySet();
        }
        return new HashSet<>(((RedisHash) entry.getValue()).keys());
    }

    @Override
    public String[] hVals(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
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
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
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
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
            return Collections.emptySet();
        }
        return new HashSet<>((Set<String>) entry.getValue().getValue());
    }

    @Override
    public boolean sIsMember(String key, String member) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
            return false;
        }
        return ((RedisSet) entry.getValue()).contains(member);
    }

    @Override
    public long sCard(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
            return 0;
        }
        return ((RedisSet) entry.getValue()).size();
    }

    @Override
    public Set<String> sInter(String... keys) {
        Set<String>[] sets = new Set[keys.length];
        for (int i = 0; i < keys.length; i++) {
            Partition p = getPartition(keys[i]);
            Entry entry = p.store.get(keys[i]);
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
            Partition p = getPartition(keys[i]);
            Entry entry = p.store.get(keys[i]);
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
            Partition p = getPartition(keys[i]);
            Entry entry = p.store.get(keys[i]);
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
        Partition srcP = getPartition(srcKey);
        Partition destP = getPartition(destKey);

        final boolean[] result = {false};
        srcP.store.computeIfPresent(srcKey, (k, entry) -> {
            if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet srcSet)) {
                return entry;
            }
            if (!srcSet.contains(member)) {
                return entry;
            }

            // 在返回新 entry 之前，先向目标写入 member
            Entry destEntry = destP.store.get(destKey);
            if (destEntry != null && destEntry.isExpired()) {
                destEntry = null;
            }
            RedisSet destSet;
            if (destEntry == null) {
                destSet = new RedisSet();
                destP.store.put(destKey, new Entry(destSet, -1));
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
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
            return null;
        }
        return ((RedisSet) entry.getValue()).randomMember();
    }

    @Override
    public String[] sRandMember(String key, long count) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired() || !(entry.getValue() instanceof RedisSet)) {
            return new String[0];
        }
        List<String> members = ((RedisSet) entry.getValue()).randomMembers(count);
        return members.toArray(new String[0]);
    }

    // ==================== 服务器操作 ====================

    @Override
    public void flushDb() {
        for (Partition p : partitions) {
            p.store.clear();
            p.expiryIndex.clear();
        }
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
        for (Partition p : partitions) {
            p.store.clear();
            p.expiryIndex.clear();
        }
    }

    @Override
    public Set<String> getAllKeys() {
        cleanupExpiredKeys();
        Set<String> keys = new HashSet<>();
        for (Partition p : partitions) {
            keys.addAll(p.store.keySet());
        }
        return keys;
    }

    // ==================== 内部方法 ====================

    private void cleanupExpiredKeys() {
        long now = System.currentTimeMillis();
        for (Partition p : partitions) {
            for (Map.Entry<String, Long> entry : p.expiryIndex.entrySet()) {
                Long expireAt = entry.getValue();
                if (expireAt != null && expireAt < now) {
                    String key = entry.getKey();
                    // CAS 删除：只有过期时间未变时才清理，避免误删刚刷新过期时间的 key
                    if (p.expiryIndex.remove(key, expireAt)) {
                        p.store.remove(key);
                    }
                }
            }
        }
    }

    private <T> T modifyList(String key, java.util.function.Function<RedisList, T> operation) {
        Partition p = getPartition(key);
        while (true) {
            Entry entry = p.store.get(key);
            if (entry == null || entry.isExpired()) {
                RedisList list = new RedisList();
                Entry newEntry = new Entry(list);
                if (p.store.putIfAbsent(key, newEntry) == null) {
                    T result = operation.apply(list);
                    if (list.isEmpty()) {
                        p.store.remove(key);
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
                p.store.remove(key);
            }

            return result;
        }
    }

    private <T> T modifyHash(String key, java.util.function.Function<RedisHash, T> operation) {
        Partition p = getPartition(key);
        while (true) {
            Entry entry = p.store.get(key);
            if (entry == null || entry.isExpired()) {
                RedisHash hash = new RedisHash();
                Entry newEntry = new Entry(hash);
                if (p.store.putIfAbsent(key, newEntry) == null) {
                    T result = operation.apply(hash);
                    if (hash.isEmpty()) {
                        p.store.remove(key);
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
                p.store.remove(key);
            }

            return result;
        }
    }

    private <T> T modifySet(String key, java.util.function.Function<RedisSet, T> operation) {
        Partition p = getPartition(key);
        while (true) {
            Entry entry = p.store.get(key);
            if (entry == null || entry.isExpired()) {
                RedisSet set = new RedisSet();
                Entry newEntry = new Entry(set);
                if (p.store.putIfAbsent(key, newEntry) == null) {
                    T result = operation.apply(set);
                    if (set.isEmpty()) {
                        p.store.remove(key);
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
                p.store.remove(key);
            }

            return result;
        }
    }

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

    /**
     * 获取分区数量
     */
    public int getNumPartitions() {
        return numPartitions;
    }

    /**
     * 获取指定分区的数据量
     */
    public long getPartitionSize(int partitionIndex) {
        if (partitionIndex < 0 || partitionIndex >= numPartitions) {
            throw new IndexOutOfBoundsException("分区索引超出范围");
        }
        return partitions[partitionIndex].store.size();
    }

    /**
     * 单个分区
     */
    private static class Partition {
        final ConcurrentHashMap<String, Entry> store;
        final ConcurrentHashMap<String, Long> expiryIndex;

        Partition() {
            this.store = new ConcurrentHashMap<>();
            this.expiryIndex = new ConcurrentHashMap<>();
        }
    }
}
