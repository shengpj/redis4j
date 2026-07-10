package com.redis4j.server;

import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.protocol.response.CommandResponse;
import io.netty.handler.codec.redis.RedisMessage;

/** Converts protocol-independent command results into Netty RESP messages. */
final class NettyResponseAdapter {
    private NettyResponseAdapter() {}

    static RedisMessage adapt(CommandResponse response) {
        if (response instanceof CommandResponse.SimpleString value) {
            return RedisMessageHelper.simpleString(value.value());
        }
        if (response instanceof CommandResponse.Error value) {
            return RedisMessageHelper.error(value.value());
        }
        if (response instanceof CommandResponse.IntegerValue value) {
            return RedisMessageHelper.integer(value.value());
        }
        if (response instanceof CommandResponse.BulkString value) {
            return RedisMessageHelper.bulkString(value.value());
        }
        if (response instanceof CommandResponse.ArrayValue value) {
            if (value.values() == null) {
                return RedisMessageHelper.nullArray();
            }
            return RedisMessageHelper.array(value.values().stream().map(NettyResponseAdapter::adapt).toList());
        }
        throw new IllegalArgumentException("Unsupported command response: " + response);
    }
}
