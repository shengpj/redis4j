package com.redis4j.storage.type;

import com.redis4j.storage.DataType;

import java.util.concurrent.atomic.AtomicLong;

/**
 * String 类型值
 */
public class RedisString implements RedisValue {

    private String value;
    private final AtomicLong version;

    public RedisString(String value) {
        this.value = value;
        this.version = new AtomicLong(0);
    }

    @Override
    public DataType getType() {
        return DataType.STRING;
    }

    @Override
    public Object getValue() {
        return value;
    }

    public String getStringValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
        this.version.incrementAndGet();
    }

    public AtomicLong getVersion() {
        return version;
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
        return "RedisString{" +
                "value='" + value + '\'' +
                ", version=" + version.get() +
                '}';
    }
}
