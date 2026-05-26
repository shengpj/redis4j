package com.redis4j.storage.type;

import com.redis4j.storage.DataType;

import java.util.*;

/**
 * Hash 类型值
 */
public class RedisHash implements RedisValue {

    private final Map<String, String> hash;

    public RedisHash() {
        this.hash = new LinkedHashMap<>();
    }

    public RedisHash(Map<String, String> initial) {
        this.hash = new LinkedHashMap<>(initial);
    }

    @Override
    public DataType getType() {
        return DataType.HASH;
    }

    @Override
    public Object getValue() {
        return hash;
    }

    public Map<String, String> getHash() {
        return hash;
    }

    public String put(String field, String value) {
        return hash.put(field, value);
    }

    public String putIfAbsent(String field, String value) {
        return hash.putIfAbsent(field, value);
    }

    public String get(String field) {
        return hash.get(field);
    }

    public String remove(String field) {
        return hash.remove(field);
    }

    public boolean containsKey(String field) {
        return hash.containsKey(field);
    }

    public long size() {
        return hash.size();
    }

    public boolean isEmpty() {
        return hash.isEmpty();
    }

    public Set<String> keys() {
        return hash.keySet();
    }

    public Collection<String> values() {
        return hash.values();
    }

    public Set<Map.Entry<String, String>> entries() {
        return hash.entrySet();
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
        return "RedisHash{" +
                "hash=" + hash +
                ", size=" + hash.size() +
                '}';
    }
}
