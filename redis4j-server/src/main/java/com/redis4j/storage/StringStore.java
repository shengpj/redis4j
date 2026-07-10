package com.redis4j.storage;

import java.util.Map;

public interface StringStore {
    void set(String key, String value);
    void setEx(String key, String value, long seconds);
    boolean setNx(String key, String value);
    String get(String key);
    String[] mGet(String... keys);
    void mSet(Map<String, String> keyValues);
    long incr(String key);
    long incrBy(String key, long delta);
    long decr(String key);
    long decrBy(String key, long delta);
    long strlen(String key);
    long append(String key, String value);
}
