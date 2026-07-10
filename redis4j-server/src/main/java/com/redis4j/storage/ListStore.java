package com.redis4j.storage;

public interface ListStore {
    long lPush(String key, String... values);
    long rPush(String key, String... values);
    String lPop(String key);
    String rPop(String key);
    long lLen(String key);
    String[] lRange(String key, long start, long stop);
    void lSet(String key, long index, String value);
    void lTrim(String key, long start, long stop);
    String lIndex(String key, long index);
}
