package com.redis4j.storage;

import com.redis4j.storage.type.*;
import com.redis4j.storage.expiration.ExpirationPolicy;
import com.redis4j.storage.expiration.IndexedExpirationPolicy;
import com.redis4j.storage.repository.ConcurrentMapEntryRepository;
import com.redis4j.storage.repository.EntryRepository;
import com.redis4j.storage.memory.EvictionPlan;
import com.redis4j.storage.memory.EvictionPolicy;
import com.redis4j.storage.memory.MemoryManagedStore;
import com.redis4j.storage.snapshot.SnapshotEntry;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 分区内存数据存储实现
 * 基于 MemoryStore，将数据分散到多个分区以减少锁竞争
 *
 * 每个分区有独立的 ConcurrentHashMap，分区选择基于 key 的 hash 值
 */
public class PartitionedMemoryStore implements DataStore, MemoryManagedStore {

    private static final int DEFAULT_PARTITIONS;
    private final Partition[] partitions;
    private final int numPartitions;
    private final ScheduledExecutorService scheduler;
    private final ConcurrentMap<String, Long> accessTimes = new ConcurrentHashMap<>();

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
        return Math.floorMod(hash(key), numPartitions);
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
        Partition p = getPartition(key);
        p.store.put(key, new Entry(new RedisString(value)));
        p.expiryIndex.remove(key);
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
        AtomicBoolean inserted = new AtomicBoolean();
        p.store.compute(key, (k, existing) -> {
            if (existing == null || existing.isExpired()) {
                inserted.set(true);
                return new Entry(new RedisString(value));
            }
            return existing;
        });
        if (inserted.get()) {
            p.expiryIndex.remove(key);
        }
        return inserted.get();
    }

    @Override
    public String get(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired()) {
            p.store.remove(key, entry);
            p.expiryIndex.remove(key);
            accessTimes.remove(key);
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
        updateExpiryIndex(p, key, entry);
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
        updateExpiryIndex(p, key, entry);
        RedisString rv = (RedisString) entry.getValue();
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
                accessTimes.remove(key);
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
            accessTimes.remove(key);
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
        long expireAt = System.currentTimeMillis() + milliseconds;
        AtomicBoolean updated = new AtomicBoolean();
        p.store.compute(key, (k, entry) -> {
            if (entry == null || entry.isExpired()) return null;
            updated.set(true);
            return new Entry(entry.getValue(), expireAt);
        });
        if (updated.get()) p.expiryIndex.put(key, expireAt);
        return updated.get();
    }

    @Override
    public long ttl(String key) {
        long result = pttl(key);
        return result >= 0 ? result / 1000 : result;
    }

    @Override
    public long pttl(String key) {
        Partition p = getPartition(key);
        Entry entry = p.store.get(key);
        if (entry == null || entry.isExpired()) {
            p.store.remove(key, entry);
            p.expiryIndex.remove(key);
            accessTimes.remove(key);
            return -2;
        }
        if (entry.isPersistent()) {
            return -1;
        }
        long remaining = entry.getExpireAt() - System.currentTimeMillis();
        if (remaining <= 0) {
            p.store.remove(key, entry);
            p.expiryIndex.remove(key);
            accessTimes.remove(key);
            return -2;
        }
        return remaining;
    }

    @Override
    public boolean persist(String key) {
        Partition p = getPartition(key);
        AtomicBoolean found = new AtomicBoolean();
        p.store.compute(key, (k, entry) -> {
            if (entry == null || entry.isExpired()) return null;
            found.set(true);
            return entry.isPersistent() ? entry : new Entry(entry.getValue());
        });
        if (found.get()) p.expiryIndex.remove(key);
        return found.get();
    }

    @Override
    public void rename(String key, String newKey) {
        if (key.equals(newKey)) return;
        int sourceIndex = getPartitionIndex(key);
        int destinationIndex = getPartitionIndex(newKey);
        Partition source = partitions[sourceIndex];
        Partition destination = partitions[destinationIndex];
        withPartitionLocks(sourceIndex, destinationIndex, () -> {
            Entry entry = source.store.get(key);
            if (entry == null || entry.isExpired()) {
                if (entry != null) source.store.remove(key, entry);
                source.expiryIndex.remove(key);
                accessTimes.remove(key);
                return;
            }
            destination.store.put(newKey, entry);
            source.store.remove(key, entry);
            source.expiryIndex.remove(key);
            updateExpiryIndex(destination, newKey, entry);
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
            long added = hash.containsKey(field) ? 0 : 1;
            hash.put(field, value);
            return added;
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
        int sourceIndex = getPartitionIndex(srcKey);
        int destinationIndex = getPartitionIndex(destKey);
        Partition source = partitions[sourceIndex];
        Partition destination = partitions[destinationIndex];
        AtomicBoolean moved = new AtomicBoolean();
        withPartitionLocks(sourceIndex, destinationIndex, () -> {
            Entry sourceEntry = source.store.get(srcKey);
            if (sourceEntry == null || sourceEntry.isExpired()
                    || !(sourceEntry.getValue() instanceof RedisSet sourceSet)
                    || !sourceSet.contains(member)) return;
            if (srcKey.equals(destKey)) {
                moved.set(true);
                return;
            }
            Entry destinationEntry = destination.store.get(destKey);
            if (destinationEntry != null && destinationEntry.isExpired()) destinationEntry = null;
            if (destinationEntry != null && !(destinationEntry.getValue() instanceof RedisSet)) return;

            RedisSet newDestination = destinationEntry == null
                    ? new RedisSet() : new RedisSet(((RedisSet) destinationEntry.getValue()).getSet());
            newDestination.add(member);
            Entry newDestinationEntry = new Entry(newDestination,
                    destinationEntry == null ? -1 : destinationEntry.getExpireAt());
            destination.store.put(destKey, newDestinationEntry);
            updateExpiryIndex(destination, destKey, newDestinationEntry);

            RedisSet newSource = new RedisSet(sourceSet.getSet());
            newSource.remove(member);
            if (newSource.isEmpty()) {
                source.store.remove(srcKey, sourceEntry);
                source.expiryIndex.remove(srcKey);
            } else {
                Entry newSourceEntry = new Entry(newSource, sourceEntry.getExpireAt());
                source.store.put(srcKey, newSourceEntry);
                updateExpiryIndex(source, srcKey, newSourceEntry);
            }
            moved.set(true);
        });
        return moved.get();
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

    // ==================== Sorted Set 操作 ====================

    @Override
    public long zAdd(String key, Map<String, Double> members) {
        return modifyZSet(key, zset -> {
            long added = 0;
            for (Map.Entry<String, Double> member : members.entrySet()) {
                if (zset.add(member.getKey(), member.getValue())) added++;
            }
            return added;
        });
    }

    @Override
    public long zRem(String key, String... members) {
        return modifyZSet(key, zset -> {
            long removed = 0;
            for (String member : members) if (zset.remove(member)) removed++;
            return removed;
        });
    }

    @Override
    public Double zScore(String key, String member) {
        RedisSortedSet zset = getZSet(key);
        return zset == null ? null : zset.score(member);
    }

    @Override
    public long zCard(String key) {
        RedisSortedSet zset = getZSet(key);
        return zset == null ? 0 : zset.size();
    }

    @Override
    public double zIncrBy(String key, double increment, String member) {
        return modifyZSet(key, zset -> zset.increment(member, increment));
    }

    @Override
    public List<ZSetStore.ScoredMember> zRange(String key, long start, long stop, boolean reverse) {
        RedisSortedSet zset = getZSet(key);
        return zset == null ? List.of() : zset.range(start, stop, reverse);
    }

    @Override
    public Long zRank(String key, String member, boolean reverse) {
        RedisSortedSet zset = getZSet(key);
        return zset == null ? null : zset.rank(member, reverse);
    }

    @Override
    public long zCount(String key, double min, boolean minInclusive, double max, boolean maxInclusive) {
        RedisSortedSet zset = getZSet(key);
        return zset == null ? 0 : zset.count(min, minInclusive, max, maxInclusive);
    }

    @Override
    public List<ZSetStore.ScoredMember> zRangeByScore(String key, double min, boolean minInclusive,
                                                      double max, boolean maxInclusive,
                                                      long offset, long count) {
        RedisSortedSet zset = getZSet(key);
        return zset == null ? List.of()
                : zset.rangeByScore(min, minInclusive, max, maxInclusive, offset, count);
    }

    @Override
    public Map<String, Double> zGetAll(String key) {
        RedisSortedSet zset = getZSet(key);
        return zset == null ? Map.of() : zset.copyScores();
    }

    private RedisSortedSet getZSet(String key) {
        Partition partition = getPartition(key);
        Entry entry = partition.store.get(key);
        if (entry == null || entry.isExpired()) return null;
        if (!(entry.getValue() instanceof RedisSortedSet))
            throw new IllegalStateException("Value is not a sorted set");
        return (RedisSortedSet) entry.getValue();
    }

    // ==================== 服务器操作 ====================

    @Override
    public void flushDb() {
        for (Partition p : partitions) {
            p.store.clear();
            p.expiryIndex.clear();
        }
        accessTimes.clear();
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
        accessTimes.clear();
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

    @Override
    public long estimatedMemoryUsage() {
        cleanupExpiredKeys();
        long total = 0;
        Set<String> liveKeys = new HashSet<>();
        for (Partition partition : partitions) {
            for (String key : partition.store.keySet()) {
                Entry entry = partition.store.get(key);
                if (entry == null || entry.isExpired()) continue;
                liveKeys.add(key);
                total = saturatedAdd(total, estimateEntryBytes(key, entry));
            }
        }
        accessTimes.keySet().retainAll(liveKeys);
        return total;
    }

    @Override
    public Map<String, SnapshotEntry> captureKeys(Set<String> keys) {
        Map<String, SnapshotEntry> captured = new HashMap<>();
        for (String key : keys) {
            Partition partition = getPartition(key);
            Entry entry = partition.store.get(key);
            if (entry != null && !entry.isExpired()) captured.put(key, snapshotEntry(key, entry));
        }
        return captured;
    }

    @Override
    public void restoreKeys(Set<String> keys, Map<String, SnapshotEntry> captured) {
        del(keys.toArray(new String[0]));
        for (SnapshotEntry snapshot : captured.values()) restoreEntry(snapshot);
    }

    @Override
    public void recordAccess(Set<String> keys) {
        long access = System.nanoTime();
        for (String key : keys) {
            Partition partition = getPartition(key);
            Entry entry = partition.store.get(key);
            if (entry != null && !entry.isExpired()) accessTimes.put(key, access);
        }
    }

    @Override
    public EvictionPlan planEvictions(long maximumBytes, EvictionPolicy policy) {
        long current = estimatedMemoryUsage();
        if (current <= maximumBytes) return new EvictionPlan(true, List.of());
        if (policy == EvictionPolicy.NOEVICTION) return new EvictionPlan(false, List.of());

        List<EvictionCandidate> candidates = new ArrayList<>();
        for (Partition partition : partitions) {
            for (String key : partition.store.keySet()) {
                Entry entry = partition.store.get(key);
                if (entry == null || entry.isExpired() || !eligible(entry, policy)) continue;
                candidates.add(new EvictionCandidate(key, estimateEntryBytes(key, entry),
                        accessTimes.getOrDefault(key, Long.MIN_VALUE), entry.getExpireAt()));
            }
        }
        switch (policy) {
            case ALLKEYS_RANDOM, VOLATILE_RANDOM -> Collections.shuffle(candidates, ThreadLocalRandom.current());
            case ALLKEYS_LRU, VOLATILE_LRU -> candidates.sort(Comparator.comparingLong(EvictionCandidate::lastAccess));
            case VOLATILE_TTL -> candidates.sort(Comparator.comparingLong(EvictionCandidate::expireAt));
            default -> { }
        }

        List<String> selected = new ArrayList<>();
        long remaining = current;
        for (EvictionCandidate candidate : candidates) {
            selected.add(candidate.key());
            remaining = Math.max(0, remaining - candidate.bytes());
            if (remaining <= maximumBytes) return new EvictionPlan(true, selected);
        }
        return new EvictionPlan(false, List.of());
    }

    private static boolean eligible(Entry entry, EvictionPolicy policy) {
        return switch (policy) {
            case VOLATILE_LRU, VOLATILE_RANDOM, VOLATILE_TTL -> !entry.isPersistent();
            default -> true;
        };
    }

    private SnapshotEntry snapshotEntry(String key, Entry entry) {
        RedisValue value = entry.getValue();
        Object copied = switch (value.getType()) {
            case STRING -> ((RedisString) value).getStringValue();
            case LIST -> List.copyOf(((RedisList) value).getList());
            case SET -> new HashSet<>(((RedisSet) value).getSet());
            case HASH -> new LinkedHashMap<>(((RedisHash) value).getHash());
            case ZSET -> new LinkedHashMap<>(((RedisSortedSet) value).copyScores());
            default -> throw new IllegalStateException("Unsupported value type: " + value.getType());
        };
        return new SnapshotEntry(key, value.getType(), copied, entry.getExpireAt());
    }

    private void restoreEntry(SnapshotEntry snapshot) {
        if (snapshot.expireAt() > 0 && snapshot.expireAt() <= System.currentTimeMillis()) return;
        RedisValue value = switch (snapshot.type()) {
            case STRING -> new RedisString((String) snapshot.value());
            case LIST -> new RedisList(new ArrayDeque<>(cast(snapshot.value())));
            case SET -> new RedisSet(new HashSet<>(cast(snapshot.value())));
            case HASH -> new RedisHash(new LinkedHashMap<>(cast(snapshot.value())));
            case ZSET -> new RedisSortedSet(new LinkedHashMap<>(cast(snapshot.value())));
            default -> throw new IllegalStateException("Unsupported value type: " + snapshot.type());
        };
        Partition partition = getPartition(snapshot.key());
        Entry restored = new Entry(value, snapshot.expireAt());
        partition.store.put(snapshot.key(), restored);
        updateExpiryIndex(partition, snapshot.key(), restored);
        accessTimes.put(snapshot.key(), System.nanoTime());
    }

    private static long estimateEntryBytes(String key, Entry entry) {
        long bytes = 72 + stringBytes(key) + (entry.isPersistent() ? 0 : 32);
        RedisValue value = entry.getValue();
        bytes = saturatedAdd(bytes, switch (value.getType()) {
            case STRING -> 40 + stringBytes(((RedisString) value).getStringValue());
            case LIST -> collectionBytes(((RedisList) value).getList(), 48);
            case SET -> collectionBytes(((RedisSet) value).getSet(), 64);
            case HASH -> hashBytes(((RedisHash) value).getHash());
            case ZSET -> zsetBytes(((RedisSortedSet) value).getScores());
            default -> 0;
        });
        return bytes;
    }

    private static long collectionBytes(Collection<String> values, long overhead) {
        long bytes = 64;
        for (String value : values) bytes = saturatedAdd(bytes, overhead + stringBytes(value));
        return bytes;
    }

    private static long hashBytes(Map<String, String> values) {
        long bytes = 64;
        for (Map.Entry<String, String> value : values.entrySet()) {
            bytes = saturatedAdd(bytes, 80 + stringBytes(value.getKey()) + stringBytes(value.getValue()));
        }
        return bytes;
    }

    private static long zsetBytes(Map<String, Double> values) {
        long bytes = 128;
        for (String member : values.keySet()) {
            bytes = saturatedAdd(bytes, 128 + stringBytes(member) + Double.BYTES);
        }
        return bytes;
    }

    private static long stringBytes(String value) {
        return value == null ? 0 : 40L + (long) value.length() * Character.BYTES;
    }

    private static long saturatedAdd(long left, long right) {
        if (right > 0 && left > Long.MAX_VALUE - right) return Long.MAX_VALUE;
        return left + right;
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object value) {
        return (T) value;
    }

    private record EvictionCandidate(String key, long bytes, long lastAccess, long expireAt) {}

    // ==================== 内部方法 ====================

    private void cleanupExpiredKeys() {
        long now = System.currentTimeMillis();
        for (Partition p : partitions) {
            for (Map.Entry<String, Long> entry : p.expiryIndex.entrySet()) {
                Long expireAt = entry.getValue();
                if (expireAt != null && expireAt < now) {
                    String key = entry.getKey();
                    // CAS 删除：只有过期时间未变时才清理，避免误删刚刷新过期时间的 key
                    Entry current = p.store.get(key);
                    if (current != null && current.getExpireAt() == expireAt && current.isExpired()
                            && p.store.remove(key, current)) {
                        p.expiryIndex.remove(key, expireAt);
                        accessTimes.remove(key);
                    } else if (current == null || current.getExpireAt() != expireAt) {
                        p.expiryIndex.remove(key, expireAt);
                    }
                }
            }
        }
    }

    private void updateExpiryIndex(Partition partition, String key, Entry entry) {
        if (entry == null || entry.isPersistent()) {
            partition.expiryIndex.remove(key);
            if (entry == null) accessTimes.remove(key);
        } else {
            partition.expiryIndex.put(key, entry.getExpireAt());
        }
    }

    private void withPartitionLocks(int firstIndex, int secondIndex, Runnable action) {
        int low = Math.min(firstIndex, secondIndex);
        int high = Math.max(firstIndex, secondIndex);
        synchronized (partitions[low].store) {
            if (low == high) {
                action.run();
            } else {
                synchronized (partitions[high].store) {
                    action.run();
                }
            }
        }
    }

    private <T> T modifyList(String key, java.util.function.Function<RedisList, T> operation) {
        Partition p = getPartition(key);
        AtomicReference<T> result = new AtomicReference<>();
        Entry updated = p.store.compute(key, (k, entry) -> {
            long expireAt = entry == null || entry.isExpired() ? -1 : entry.getExpireAt();
            if (entry != null && !entry.isExpired() && !(entry.getValue() instanceof RedisList))
                throw new IllegalStateException("Value is not a list");
            RedisList list = entry == null || entry.isExpired()
                    ? new RedisList() : new RedisList(((RedisList) entry.getValue()).getList());
            result.set(operation.apply(list));
            return list.isEmpty() ? null : new Entry(list, expireAt);
        });
        updateExpiryIndex(p, key, updated);
        return result.get();
    }

    private <T> T modifyHash(String key, java.util.function.Function<RedisHash, T> operation) {
        Partition p = getPartition(key);
        AtomicReference<T> result = new AtomicReference<>();
        Entry updated = p.store.compute(key, (k, entry) -> {
            long expireAt = entry == null || entry.isExpired() ? -1 : entry.getExpireAt();
            if (entry != null && !entry.isExpired() && !(entry.getValue() instanceof RedisHash))
                throw new IllegalStateException("Value is not a hash");
            RedisHash hash = entry == null || entry.isExpired()
                    ? new RedisHash() : new RedisHash(((RedisHash) entry.getValue()).getHash());
            result.set(operation.apply(hash));
            return hash.isEmpty() ? null : new Entry(hash, expireAt);
        });
        updateExpiryIndex(p, key, updated);
        return result.get();
    }

    private <T> T modifySet(String key, java.util.function.Function<RedisSet, T> operation) {
        Partition p = getPartition(key);
        AtomicReference<T> result = new AtomicReference<>();
        Entry updated = p.store.compute(key, (k, entry) -> {
            long expireAt = entry == null || entry.isExpired() ? -1 : entry.getExpireAt();
            if (entry != null && !entry.isExpired() && !(entry.getValue() instanceof RedisSet))
                throw new IllegalStateException("Value is not a set");
            RedisSet set = entry == null || entry.isExpired()
                    ? new RedisSet() : new RedisSet(((RedisSet) entry.getValue()).getSet());
            result.set(operation.apply(set));
            return set.isEmpty() ? null : new Entry(set, expireAt);
        });
        updateExpiryIndex(p, key, updated);
        return result.get();
    }

    private <T> T modifyZSet(String key, java.util.function.Function<RedisSortedSet, T> operation) {
        Partition partition = getPartition(key);
        AtomicReference<T> result = new AtomicReference<>();
        Entry updated = partition.store.compute(key, (k, entry) -> {
            long expireAt = entry == null || entry.isExpired() ? -1 : entry.getExpireAt();
            if (entry != null && !entry.isExpired() && !(entry.getValue() instanceof RedisSortedSet))
                throw new IllegalStateException("Value is not a sorted set");
            RedisSortedSet zset = entry == null || entry.isExpired()
                    ? new RedisSortedSet()
                    : new RedisSortedSet(((RedisSortedSet) entry.getValue()).copyScores());
            result.set(operation.apply(zset));
            return zset.isEmpty() ? null : new Entry(zset, expireAt);
        });
        updateExpiryIndex(partition, key, updated);
        return result.get();
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
        final EntryRepository store;
        final ExpirationPolicy expiryIndex;

        Partition() {
            this.store = new ConcurrentMapEntryRepository();
            this.expiryIndex = new IndexedExpirationPolicy();
        }
    }
}
