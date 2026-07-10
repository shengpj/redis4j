package com.redis4j.storage;

import java.util.Set;

public interface KeyStore {
    long del(String... keys);
    boolean exists(String key);
    long exists(String... keys);
    boolean expire(String key, long seconds);
    boolean expireMs(String key, long milliseconds);
    long ttl(String key);
    long pttl(String key);
    boolean persist(String key);
    void rename(String key, String newKey);
    DataType type(String key);
    Set<String> keys(String pattern);
    long dbSize();
    void flushDb();
    void flushAll();
}
