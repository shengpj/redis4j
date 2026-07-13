package com.redis4j.server;

import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.storage.MemoryStore;
import com.redis4j.storage.memory.EvictionPolicy;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ObservabilityCommandsTest {

    @Test
    void slowLogTruncatesArgumentsHonorsCapacityAndCanBeDisabled() {
        SlowLog log = new SlowLog(0, 1);
        String[] manyArguments = new String[100];
        Arrays.fill(manyArguments, "x".repeat(1_000));
        log.record("SET", manyArguments, 10, "client", "");
        SlowLog.Entry first = log.get(1).get(0);
        assertEquals(32, first.arguments().size());
        assertTrue(first.arguments().get(1).contains("more characters"));
        assertTrue(first.arguments().get(1).length() < 200);
        assertTrue(first.arguments().get(31).contains("more arguments"));

        log.record("GET", new String[]{"key"}, 5, "client", "");
        assertEquals(1, log.length());
        assertEquals("GET", log.get(1).get(0).arguments().get(0));

        SlowLog disabled = new SlowLog(-1, 128);
        disabled.record("GET", new String[]{"key"}, Long.MAX_VALUE, "client", "");
        assertEquals(0, disabled.length());
    }

    @Test
    void slowLogIsBoundedAndSupportsGetLenAndReset() throws Exception {
        ServerConfig config = new ServerConfig();
        config.setSlowLogSlowerThanMicros(0);
        config.setSlowLogMaxLen(2);
        try (Fixture fixture = new Fixture(config)) {
            fixture.channel.writeInbound(request("SET", "key", "value"));
            releaseDeep(awaitOutbound(fixture.channel));
            fixture.channel.writeInbound(request("GET", "key"));
            releaseDeep(awaitOutbound(fixture.channel));
            fixture.channel.writeInbound(request("DBSIZE"));
            releaseDeep(awaitOutbound(fixture.channel));

            fixture.channel.writeInbound(request("SLOWLOG", "LEN"));
            assertEquals(2, ((IntegerRedisMessage) awaitOutbound(fixture.channel)).value());

            fixture.channel.writeInbound(request("SLOWLOG", "GET", "1"));
            ArrayRedisMessage outer = (ArrayRedisMessage) awaitOutbound(fixture.channel);
            assertEquals(1, outer.children().size());
            ArrayRedisMessage entry = (ArrayRedisMessage) outer.children().get(0);
            assertEquals(6, entry.children().size());
            ArrayRedisMessage command = (ArrayRedisMessage) entry.children().get(3);
            assertEquals("DBSIZE", RedisMessageHelper.extractString(command.children().get(0)));
            assertTrue(((IntegerRedisMessage) entry.children().get(2)).value() >= 0);
            releaseDeep(outer);

            fixture.channel.writeInbound(request("SLOWLOG", "RESET"));
            RedisMessage reset = awaitOutbound(fixture.channel);
            assertEquals("OK", ((SimpleStringRedisMessage) reset).content());
            releaseDeep(reset);
            fixture.channel.writeInbound(request("SLOWLOG", "LEN"));
            assertEquals(0, ((IntegerRedisMessage) awaitOutbound(fixture.channel)).value());
        }
    }

    @Test
    void configGetReturnsOnlyMatchingEffectiveValues() throws Exception {
        ServerConfig config = new ServerConfig();
        config.setMaxMemoryBytes(4096);
        config.setMaxMemoryPolicy(EvictionPolicy.ALLKEYS_LRU);
        config.setSlowLogMaxLen(64);
        config.setMaxClients(250);
        config.setClientIdleTimeoutSeconds(45);
        try (Fixture fixture = new Fixture(config)) {
            fixture.channel.writeInbound(request("CONFIG", "GET", "slowlog-*"));
            ArrayRedisMessage values = (ArrayRedisMessage) awaitOutbound(fixture.channel);
            assertArrayEquals(new String[]{
                    "slowlog-log-slower-than", "10000", "slowlog-max-len", "64"
            }, strings(values));
            releaseDeep(values);

            fixture.channel.writeInbound(request("CONFIG", "GET", "maxmemory*"));
            ArrayRedisMessage memory = (ArrayRedisMessage) awaitOutbound(fixture.channel);
            assertArrayEquals(new String[]{"maxmemory", "4096", "maxmemory-policy", "allkeys-lru"},
                    strings(memory));
            releaseDeep(memory);

            fixture.channel.writeInbound(request("CONFIG", "GET", "maxclients"));
            ArrayRedisMessage clients = (ArrayRedisMessage) awaitOutbound(fixture.channel);
            assertArrayEquals(new String[]{"maxclients", "250"}, strings(clients));
            releaseDeep(clients);

            fixture.channel.writeInbound(request("CONFIG", "GET", "timeout"));
            ArrayRedisMessage timeout = (ArrayRedisMessage) awaitOutbound(fixture.channel);
            assertArrayEquals(new String[]{"timeout", "45"}, strings(timeout));
            releaseDeep(timeout);

            fixture.channel.writeInbound(request("CONFIG", "SET", "maxmemory", "1"));
            RedisMessage error = awaitOutbound(fixture.channel);
            assertInstanceOf(ErrorRedisMessage.class, error);
            releaseDeep(error);
        }
    }

    @Test
    void clientListShowsSubscriptionsAndRemovesDisconnectedClients() throws Exception {
        ServerConfig config = new ServerConfig();
        MemoryStore store = new MemoryStore();
        CommandRegistry registry = new CommandRegistry(store);
        PubSubBroker broker = new PubSubBroker();
        ServerObservability observability = new ServerObservability(config);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(16));
        EmbeddedChannel subscriber = new EmbeddedChannel(
                new NettyCodecHandler(registry, executor, broker, observability, 16));
        EmbeddedChannel observer = new EmbeddedChannel(
                new NettyCodecHandler(registry, executor, broker, observability, 16));
        try {
            subscriber.writeInbound(request("SUBSCRIBE", "events"));
            releaseDeep(awaitOutbound(subscriber));

            observer.writeInbound(request("CLIENT", "LIST"));
            RedisMessage first = awaitOutbound(observer);
            String listing = bulk(first);
            assertEquals(2, listing.lines().count());
            assertTrue(listing.contains(" flags=P "));
            assertTrue(listing.contains(" sub=1 "));
            assertTrue(listing.contains(" cmd=client\n"));
            releaseDeep(first);

            subscriber.close();
            observer.writeInbound(request("CLIENT", "LIST"));
            RedisMessage second = awaitOutbound(observer);
            assertEquals(1, bulk(second).lines().count());
            releaseDeep(second);
            assertEquals(1, observability.clients().size());
        } finally {
            subscriber.finishAndReleaseAll();
            observer.finishAndReleaseAll();
            executor.shutdownNow();
            store.close();
        }
    }

    private static ArrayRedisMessage request(String... arguments) {
        return new ArrayRedisMessage(Arrays.stream(arguments)
                .map(RedisMessageHelper::bulkString)
                .toList());
    }

    private static RedisMessage awaitOutbound(EmbeddedChannel channel) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        RedisMessage message;
        while ((message = channel.readOutbound()) == null && System.nanoTime() < deadline) {
            channel.runPendingTasks();
            Thread.sleep(5);
        }
        assertNotNull(message);
        return message;
    }

    private static String[] strings(ArrayRedisMessage array) {
        return array.children().stream().map(RedisMessageHelper::extractString).toArray(String[]::new);
    }

    private static String bulk(RedisMessage message) {
        return ((FullBulkStringRedisMessage) message).content().toString(StandardCharsets.UTF_8);
    }

    private static void releaseDeep(RedisMessage message) {
        if (message instanceof ArrayRedisMessage array) array.children().forEach(ObservabilityCommandsTest::releaseDeep);
        else ReferenceCountUtil.release(message);
    }

    private static final class Fixture implements AutoCloseable {
        private final MemoryStore store = new MemoryStore();
        private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(16));
        private final EmbeddedChannel channel;

        private Fixture(ServerConfig config) {
            channel = new EmbeddedChannel(new NettyCodecHandler(new CommandRegistry(store), executor,
                    new PubSubBroker(), new ServerObservability(config), 16));
        }

        @Override
        public void close() {
            channel.finishAndReleaseAll();
            executor.shutdownNow();
            store.close();
        }
    }
}
