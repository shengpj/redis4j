package com.redis4j.server;

import com.redis4j.command.Command;
import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.storage.MemoryStore;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NettyCodecHandlerTest {

    @Test
    void adaptsRealCommandResponsesToNettyMessages() throws Exception {
        MemoryStore store = new MemoryStore();
        CommandRegistry registry = new CommandRegistry(store);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2, 2, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(16));
        EmbeddedChannel channel = new EmbeddedChannel(new NettyCodecHandler(registry, executor));
        try {
            channel.writeInbound(request("SET", "key", "value"));
            channel.writeInbound(request("GET", "key"));
            RedisMessage setResponse = awaitOutbound(channel);
            RedisMessage getResponse = awaitOutbound(channel);
            assertEquals("OK", ((SimpleStringRedisMessage) setResponse).content());
            assertEquals("value", ((FullBulkStringRedisMessage) getResponse).content()
                    .toString(java.nio.charset.StandardCharsets.UTF_8));
            ReferenceCountUtil.release(setResponse);
            ReferenceCountUtil.release(getResponse);
        } finally {
            channel.finishAndReleaseAll();
            executor.shutdownNow();
            store.close();
        }
    }

    @Test
    void preservesCommandCompletionOrderForOneConnection() throws Exception {
        MemoryStore store = new MemoryStore();
        CommandRegistry registry = new CommandRegistry(store);
        registry.register(command("SLOW", 100));
        registry.register(command("FAST", 0));
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2, 2, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(16));
        EmbeddedChannel channel = new EmbeddedChannel(new NettyCodecHandler(registry, executor));

        try {
            channel.writeInbound(request("SLOW"));
            channel.writeInbound(request("FAST"));

            RedisMessage first = awaitOutbound(channel);
            RedisMessage second = awaitOutbound(channel);
            assertEquals("SLOW", ((SimpleStringRedisMessage) first).content());
            assertEquals("FAST", ((SimpleStringRedisMessage) second).content());
            ReferenceCountUtil.release(first);
            ReferenceCountUtil.release(second);
        } finally {
            channel.finishAndReleaseAll();
            executor.shutdownNow();
            store.close();
        }
    }

    @Test
    void publishesAcrossConnectionsAndCleansSubscriptionsOnDisconnect() throws Exception {
        MemoryStore store = new MemoryStore();
        CommandRegistry registry = new CommandRegistry(store);
        PubSubBroker broker = new PubSubBroker();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2, 2, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(16));
        EmbeddedChannel subscriber = new EmbeddedChannel(new NettyCodecHandler(registry, executor, broker, 16));
        EmbeddedChannel publisher = new EmbeddedChannel(new NettyCodecHandler(registry, executor, broker, 16));
        try {
            subscriber.writeInbound(request("SUBSCRIBE", "events"));
            RedisMessage subscribeAck = awaitOutbound(subscriber);
            assertArray(subscribeAck, "subscribe", "events", "1");
            releaseDeep(subscribeAck);

            subscriber.writeInbound(request("GET", "key"));
            RedisMessage rejected = awaitOutbound(subscriber);
            assertEquals("ERR only SUBSCRIBE, UNSUBSCRIBE and PING are allowed in subscribed mode",
                    ((ErrorRedisMessage) rejected).content());
            releaseDeep(rejected);

            publisher.writeInbound(request("PUBLISH", "events", "created"));
            RedisMessage publishCount = awaitOutbound(publisher);
            assertEquals(1, ((IntegerRedisMessage) publishCount).value());
            releaseDeep(publishCount);
            RedisMessage pushed = awaitOutbound(subscriber);
            assertArray(pushed, "message", "events", "created");
            releaseDeep(pushed);

            subscriber.close();
            publisher.writeInbound(request("PUBLISH", "events", "after-close"));
            RedisMessage afterClose = awaitOutbound(publisher);
            assertEquals(0, ((IntegerRedisMessage) afterClose).value());
            releaseDeep(afterClose);
            assertNull(publisher.readOutbound());
        } finally {
            subscriber.finishAndReleaseAll();
            publisher.finishAndReleaseAll();
            executor.shutdownNow();
            store.close();
        }
    }

    @Test
    void unsubscribeReturnsConnectionToNormalCommandMode() throws Exception {
        MemoryStore store = new MemoryStore();
        CommandRegistry registry = new CommandRegistry(store);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(16));
        EmbeddedChannel channel = new EmbeddedChannel(
                new NettyCodecHandler(registry, executor, new PubSubBroker(), 16));
        try {
            channel.writeInbound(request("SUBSCRIBE", "one", "two"));
            RedisMessage first = awaitOutbound(channel);
            RedisMessage second = awaitOutbound(channel);
            assertArray(first, "subscribe", "one", "1");
            assertArray(second, "subscribe", "two", "2");
            releaseDeep(first);
            releaseDeep(second);

            channel.writeInbound(request("PING", "alive"));
            RedisMessage pong = awaitOutbound(channel);
            assertArray(pong, "pong", "alive");
            releaseDeep(pong);

            channel.writeInbound(request("UNSUBSCRIBE"));
            RedisMessage unsubscribeOne = awaitOutbound(channel);
            RedisMessage unsubscribeTwo = awaitOutbound(channel);
            releaseDeep(unsubscribeOne);
            releaseDeep(unsubscribeTwo);

            channel.writeInbound(request("SET", "key", "value"));
            RedisMessage set = awaitOutbound(channel);
            assertEquals("OK", ((SimpleStringRedisMessage) set).content());
            releaseDeep(set);
        } finally {
            channel.finishAndReleaseAll();
            executor.shutdownNow();
            store.close();
        }
    }

    @Test
    void brokerMaintainsConsistentIndexesDuringConcurrentSubscriptions() throws Exception {
        PubSubBroker broker = new PubSubBroker();
        EmbeddedChannel channel = new EmbeddedChannel();
        int threads = 8;
        int topicsPerThread = 50;
        ExecutorService workers = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        try {
            for (int thread = 0; thread < threads; thread++) {
                int threadIndex = thread;
                workers.execute(() -> {
                    try {
                        start.await();
                        for (int topic = 0; topic < topicsPerThread; topic++) {
                            broker.subscribe(channel, "topic-" + threadIndex + '-' + topic);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            start.countDown();
            workers.shutdown();
            assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
            assertEquals(threads * topicsPerThread, broker.subscriptionCount(channel));

            broker.remove(channel);
            assertEquals(0, broker.subscriptionCount(channel));
            assertEquals(0, broker.publish("topic-0-0", "payload"));
        } finally {
            workers.shutdownNow();
            channel.finishAndReleaseAll();
        }
    }

    private static Command command(String name, long delayMillis) {
        return new Command() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public int getArity() {
                return 1;
            }

            @Override
            public CommandResponse execute(String[] args) {
                if (delayMillis > 0) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return CommandResponses.simpleString(name);
            }
        };
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

    private static void assertArray(RedisMessage message, String... expected) {
        ArrayRedisMessage array = (ArrayRedisMessage) message;
        String[] actual = array.children().stream()
                .map(value -> value instanceof IntegerRedisMessage integer
                        ? Long.toString(integer.value()) : RedisMessageHelper.extractString(value))
                .toArray(String[]::new);
        org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual);
    }

    private static void releaseDeep(RedisMessage message) {
        if (message instanceof ArrayRedisMessage array) {
            array.children().forEach(NettyCodecHandlerTest::releaseDeep);
        } else {
            ReferenceCountUtil.release(message);
        }
    }
}
