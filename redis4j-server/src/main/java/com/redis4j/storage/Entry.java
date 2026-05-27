package com.redis4j.storage;

import com.redis4j.storage.type.RedisValue;

/**
 * 数据条目包装器
 * 包含值和过期时间信息
 */
public class Entry {
    private final RedisValue value;
    private final long expireAt; // -1 表示永不过期

    public Entry(RedisValue value) {
        this(value, -1);
    }

    public Entry(RedisValue value, long expireAt) {
        this.value = value;
        this.expireAt = expireAt;
    }

    public RedisValue getValue() {
        return value;
    }

    public long getExpireAt() {
        return expireAt;
    }

    public boolean isExpired() {
        return expireAt > 0 && expireAt < System.currentTimeMillis();
    }

    public boolean isPersistent() {
        return expireAt < 0;
    }
}
