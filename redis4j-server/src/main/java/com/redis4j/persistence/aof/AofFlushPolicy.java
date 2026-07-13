package com.redis4j.persistence.aof;

import java.util.Locale;

/** AOF 刷盘策略，对应 Redis 的 appendfsync 配置。 */
public enum AofFlushPolicy {
    ALWAYS,
    EVERYSEC,
    NO;

    public static AofFlushPolicy parse(String value) {
        return valueOf(value.trim().toUpperCase(Locale.ROOT));
    }
}
