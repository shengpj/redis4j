package com.redis4j.server;

import com.redis4j.command.Command;
import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.storage.MemoryStore;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
}
