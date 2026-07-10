package com.redis4j.storage.expiration;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class IndexedExpirationPolicy implements ExpirationPolicy {
    private final ConcurrentHashMap<String, Long> deadlines = new ConcurrentHashMap<>();

    @Override public Long put(String key, long expireAt) { return deadlines.put(key, expireAt); }
    @Override public Long remove(String key) { return deadlines.remove(key); }
    @Override public boolean remove(String key, long expireAt) { return deadlines.remove(key, expireAt); }
    @Override public Set<Map.Entry<String, Long>> entrySet() { return deadlines.entrySet(); }
    @Override public void clear() { deadlines.clear(); }
}
