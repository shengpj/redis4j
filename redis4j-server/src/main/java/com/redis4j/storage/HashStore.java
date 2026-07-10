package com.redis4j.storage;

import java.util.Map;
import java.util.Set;

public interface HashStore {
    long hSet(String key, String field, String value);
    boolean hSetNx(String key, String field, String value);
    String hGet(String key, String field);
    Map<String, String> hGetAll(String key);
    long hDel(String key, String... fields);
    boolean hExists(String key, String field);
    long hLen(String key);
    Set<String> hKeys(String key);
    String[] hVals(String key);
    long hMSet(String key, Map<String, String> fieldValues);
    String[] hMGet(String key, String... fields);
    long hIncrBy(String key, String field, long delta);
}
