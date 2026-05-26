package com.redis4j.storage.type;

import com.redis4j.storage.DataType;

import java.util.*;

/**
 * Set 类型值
 */
public class RedisSet implements RedisValue {

    private final Set<String> set;

    public RedisSet() {
        this.set = new HashSet<>();
    }

    public RedisSet(Set<String> initial) {
        this.set = new HashSet<>(initial);
    }

    @Override
    public DataType getType() {
        return DataType.SET;
    }

    @Override
    public Object getValue() {
        return set;
    }

    public Set<String> getSet() {
        return set;
    }

    public boolean add(String... members) {
        boolean added = false;
        for (String member : members) {
            added = set.add(member) || added;
        }
        return added;
    }

    public boolean remove(String... members) {
        boolean removed = false;
        for (String member : members) {
            removed = set.remove(member) || removed;
        }
        return removed;
    }

    public boolean contains(String member) {
        return set.contains(member);
    }

    public long size() {
        return set.size();
    }

    public boolean isEmpty() {
        return set.isEmpty();
    }

    public Iterator<String> iterator() {
        return set.iterator();
    }

    /**
     * 交集
     */
    public static Set<String> inter(Set<String>... sets) {
        if (sets == null || sets.length == 0) {
            return Collections.emptySet();
        }

        Set<String> result = new HashSet<>(sets[0]);
        for (int i = 1; i < sets.length; i++) {
            result.retainAll(sets[i]);
        }
        return result;
    }

    /**
     * 并集
     */
    public static Set<String> union(Set<String>... sets) {
        if (sets == null || sets.length == 0) {
            return Collections.emptySet();
        }

        Set<String> result = new HashSet<>();
        for (Set<String> set : sets) {
            result.addAll(set);
        }
        return result;
    }

    /**
     * 差集（第一个集合减去后面的集合）
     */
    public static Set<String> diff(Set<String>... sets) {
        if (sets == null || sets.length == 0) {
            return Collections.emptySet();
        }

        Set<String> result = new HashSet<>(sets[0]);
        for (int i = 1; i < sets.length; i++) {
            result.removeAll(sets[i]);
        }
        return result;
    }

    /**
     * 随机获取一个元素
     */
    public String randomMember() {
        if (set.isEmpty()) {
            return null;
        }
        List<String> list = new ArrayList<>(set);
        return list.get(new Random().nextInt(list.size()));
    }

    /**
     * 随机获取多个元素
     */
    public List<String> randomMembers(long count) {
        if (set.isEmpty() || count <= 0) {
            return Collections.emptyList();
        }

        List<String> result = new ArrayList<>();
        List<String> list = new ArrayList<>(set);

        if (count >= list.size()) {
            return list;
        }

        Random random = new Random();
        for (int i = 0; i < count; i++) {
            result.add(list.get(random.nextInt(list.size())));
        }
        return result;
    }

    /**
     * 随机弹出一个元素并返回
     */
    public String pop() {
        String member = randomMember();
        if (member != null) {
            set.remove(member);
        }
        return member;
    }

    @Override
    public boolean isExpired() {
        return false;
    }

    @Override
    public long getTtl() {
        return -1;
    }

    @Override
    public String toString() {
        return "RedisSet{" +
                "set=" + set +
                ", size=" + set.size() +
                '}';
    }
}
