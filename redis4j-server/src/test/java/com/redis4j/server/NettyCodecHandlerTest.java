package com.redis4j.server;

import com.redis4j.command.Command;
import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.storage.MemoryStore;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class NettyCodecHandlerTest {

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
            public RedisMessage execute(String[] args) {
                if (delayMillis > 0) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return RedisMessageHelper.simpleString(name);
            }
        };
    }

    private static ArrayRedisMessage request(String command) {
        return new ArrayRedisMessage(List.of(RedisMessageHelper.bulkString(command)));
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
