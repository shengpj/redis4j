package com.redis4j.server;

import com.redis4j.protocol.RedisArrayAggregator;

/** Server-side compatibility name for the shared RESP array aggregator. */
public class RedisMessageAggregator extends RedisArrayAggregator {
    public RedisMessageAggregator() {
        super();
    }

    public RedisMessageAggregator(int maxArrayLength) {
        super(maxArrayLength);
    }
}
