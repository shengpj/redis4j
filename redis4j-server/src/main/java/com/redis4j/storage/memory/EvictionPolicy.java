package com.redis4j.storage.memory;

import java.util.Locale;

/** Redis 风格的内存淘汰策略。 */
public enum EvictionPolicy {
    NOEVICTION,
    ALLKEYS_LRU,
    ALLKEYS_RANDOM,
    VOLATILE_LRU,
    VOLATILE_RANDOM,
    VOLATILE_TTL;

    public static EvictionPolicy parse(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("maxmemory policy is required");
        return valueOf(value.trim().replace('-', '_').toUpperCase(Locale.ROOT));
    }
}
