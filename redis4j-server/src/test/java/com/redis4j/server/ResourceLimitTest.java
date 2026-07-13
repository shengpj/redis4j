package com.redis4j.server;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.redis.BulkStringHeaderRedisMessage;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ResourceLimitTest {
    @Test
    void rejectsBulkStringsBeforeAggregation() {
        EmbeddedChannel channel = new EmbeddedChannel(new RedisMessageSizeLimiter(8));
        assertThrows(TooLongFrameException.class,
                () -> channel.writeInbound(new BulkStringHeaderRedisMessage(9)));
        channel.finishAndReleaseAll();
    }

    @Test
    void allowsBulkStringsWithinLimit() {
        EmbeddedChannel channel = new EmbeddedChannel(new RedisMessageSizeLimiter(8));
        assertTrue(channel.writeInbound(new BulkStringHeaderRedisMessage(8)));
        ReferenceCountUtil.release(channel.readInbound());
        channel.finishAndReleaseAll();
    }

    @Test
    void validatesResourceLimits() {
        ServerConfig config = new ServerConfig();
        assertThrows(IllegalArgumentException.class, () -> config.setMaxFrameLength(0));
        assertThrows(IllegalArgumentException.class, () -> config.setMaxArrayLength(0));
        assertThrows(IllegalArgumentException.class, () -> config.setMaxPendingCommandsPerConnection(0));
        assertThrows(IllegalArgumentException.class, () -> config.setCommandQueueCapacity(0));
    }
}
