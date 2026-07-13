package com.redis4j.server;

import com.redis4j.command.CommandRegistry;
import com.redis4j.storage.MemoryStore;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.redis.BulkStringHeaderRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import com.redis4j.storage.memory.EvictionPolicy;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
        assertTrue(config.isAofUseRdbPreamble());
        assertEquals(0, config.getMaxMemoryBytes());
        assertEquals(EvictionPolicy.NOEVICTION, config.getMaxMemoryPolicy());
        assertThrows(IllegalArgumentException.class, () -> config.setMaxFrameLength(0));
        assertThrows(IllegalArgumentException.class, () -> config.setMaxArrayLength(0));
        assertThrows(IllegalArgumentException.class, () -> config.setMaxPendingCommandsPerConnection(0));
        assertThrows(IllegalArgumentException.class, () -> config.setCommandQueueCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> config.setMaxClients(0));
        assertThrows(IllegalArgumentException.class, () -> config.setClientIdleTimeoutSeconds(-1));
        assertThrows(IllegalArgumentException.class, () -> config.setClientOutputBufferLimitNormal(-1));
        assertThrows(IllegalArgumentException.class, () -> config.setClientOutputBufferLimitPubSub(-1));
        assertThrows(IllegalArgumentException.class, () -> config.setAofQueueCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> config.setAutoAofRewriteMinSize(-1));
        assertThrows(IllegalArgumentException.class, () -> config.setAutoAofRewritePercentage(-1));
        assertThrows(IllegalArgumentException.class, () -> config.setMaxMemoryBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> config.setMaxMemoryPolicy(null));
        assertThrows(IllegalArgumentException.class, () -> config.setSlowLogSlowerThanMicros(-2));
        assertThrows(IllegalArgumentException.class, () -> config.setSlowLogMaxLen(-1));
        assertEquals(10_000, config.getSlowLogSlowerThanMicros());
        assertEquals(128, config.getSlowLogMaxLen());
        assertEquals(10_000, config.getMaxClients());
        assertEquals(30, config.getClientIdleTimeoutSeconds());
        assertEquals(8L * 1024 * 1024, config.getClientOutputBufferLimitNormal());
        assertEquals(32L * 1024 * 1024, config.getClientOutputBufferLimitPubSub());
        assertEquals(EvictionPolicy.ALLKEYS_LRU, EvictionPolicy.parse("allkeys-lru"));
    }

    @Test
    void rejectsConnectionsOverMaxClientsAndReusesReleasedPermit() {
        ServerConfig config = new ServerConfig();
        config.setMaxClients(1);
        ServerObservability observability = new ServerObservability(config);
        PubSubBroker broker = new PubSubBroker();
        MemoryStore store = new MemoryStore();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(4));
        EmbeddedChannel first = new EmbeddedChannel(new NettyCodecHandler(
                new CommandRegistry(store), executor, broker, observability, 4));
        EmbeddedChannel rejected = new EmbeddedChannel(new NettyCodecHandler(
                new CommandRegistry(store), executor, broker, observability, 4));
        EmbeddedChannel replacement = null;
        try {
            RedisMessage error = rejected.readOutbound();
            assertInstanceOf(ErrorRedisMessage.class, error);
            assertEquals("ERR max number of clients reached", ((ErrorRedisMessage) error).content());
            ReferenceCountUtil.release(error);
            assertEquals(new ClientConnectionMetrics(1, 1, 1), observability.connectionMetrics());

            first.close();
            replacement = new EmbeddedChannel(new NettyCodecHandler(
                    new CommandRegistry(store), executor, broker, observability, 4));
            assertTrue(replacement.isActive());
            assertEquals(new ClientConnectionMetrics(1, 1, 1), observability.connectionMetrics());
        } finally {
            first.finishAndReleaseAll();
            rejected.finishAndReleaseAll();
            if (replacement != null) replacement.finishAndReleaseAll();
            executor.shutdownNow();
            store.close();
        }
        assertEquals(0, observability.connectionMetrics().connectedClients());
    }

    @Test
    void appliesSeparateNormalAndPubSubOutputLimits() {
        PubSubBroker broker = new PubSubBroker();
        EmbeddedChannel normal = new EmbeddedChannel(new ClientOutputBufferLimitHandler(broker, 8, 16));
        EmbeddedChannel subscriber = new EmbeddedChannel(new ClientOutputBufferLimitHandler(broker, 8, 16));
        try {
            assertThrows(TooLongFrameException.class,
                    () -> normal.writeOutbound(Unpooled.wrappedBuffer(new byte[9])));
            assertFalse(normal.isActive());

            broker.subscribe(subscriber, "events");
            assertTrue(subscriber.writeOutbound(Unpooled.wrappedBuffer(new byte[12])));
            ByteBuf accepted = subscriber.readOutbound();
            assertEquals(12, accepted.readableBytes());
            accepted.release();
            assertTrue(subscriber.isActive());
        } finally {
            normal.finishAndReleaseAll();
            subscriber.finishAndReleaseAll();
        }
    }
}
