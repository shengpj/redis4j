package com.redis4j.storage;

import com.redis4j.storage.type.RedisValue;

/**
 * 数据条目包装类
 * 包含值和过期时间信息
 */
public class Entry {
    private final RedisValue value;
    private final long expireAt; // -1 表示永不过期

    Entry(RedisValue value) {
        this(value, -1);
    }

    Entry(RedisValue value, long expireAt) {
        this.value = value;
        this.expireAt = expireAt;
    }

    RedisValue getValue() {
        return value;
    }

    long getExpireAt() {
        return expireAt;
    }

    boolean isExpired() {
        return expireAt > 0 && expireAt < System.currentTimeMillis();
    }

    boolean isPersistent() {
        return expireAt < 0;
    }
}
