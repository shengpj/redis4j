package com.redis4j.storage.expiration;

import java.util.Map;
import java.util.Set;

/** Index strategy for actively expiring keys. */
public interface ExpirationPolicy {
    Long put(String key, long expireAt);
    Long remove(String key);
    boolean remove(String key, long expireAt);
    Set<Map.Entry<String, Long>> entrySet();
    void clear();
}
