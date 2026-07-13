package com.redis4j.client;

import com.redis4j.protocol.RedisArrayAggregator;
import com.redis4j.protocol.RedisMessageHelper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.codec.redis.RedisMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RedisClientTest {
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private int port;

    @BeforeEach
    void startServer() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(1);
        serverChannel = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new RedisDecoder());
                        ch.pipeline().addLast(new RedisBulkStringAggregator());
                        ch.pipeline().addLast(new RedisArrayAggregator());
                        ch.pipeline().addLast(new RedisEncoder());
                        ch.pipeline().addLast(new EchoHandler());
                    }
                })
                .bind("127.0.0.1", 0)
                .sync()
                .channel();
        port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @AfterEach
    void stopServer() {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    @Test
    void matchesConcurrentRequestsWithTheirResponses() throws Exception {
        RedisClient client = new RedisClient("127.0.0.1", port);
        client.connect();
        ExecutorService callers = Executors.newFixedThreadPool(8);
        try {
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String expected = "value-" + i;
                results.add(callers.submit(() -> RedisMessageHelper.extractString(
                        client.sendCommandFuture("ECHO", expected).get())));
            }
            for (int i = 0; i < results.size(); i++) {
                assertEquals("value-" + i, results.get(i).get(5, TimeUnit.SECONDS));
            }
        } finally {
            callers.shutdownNow();
            client.disconnect();
        }
    }

    @Test
    void disconnectFailsPendingRequestImmediately() throws Exception {
        RedisClient client = new RedisClient("127.0.0.1", port);
        client.connect();
        var response = client.sendCommandFuture("BLOCK");
        client.disconnect();
        assertTrue(response.isCompletedExceptionally());
    }

    @Test
    void scanParsesCursorAndNestedValues() throws Exception {
        RedisClient client = new RedisClient("127.0.0.1", port);
        client.connect();
        try {
            RedisCommands.ScanResult result = new RedisCommands(client)
                    .scan("0", "MATCH", "user:*", "COUNT", "2");

            assertEquals("7", result.cursor());
            assertArrayEquals(new String[]{"user:1", "user:2"}, result.values());
        } finally {
            client.disconnect();
        }
    }

    private static final class EchoHandler extends SimpleChannelInboundHandler<RedisMessage> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) {
            ArrayRedisMessage request = (ArrayRedisMessage) msg;
            String command = RedisMessageHelper.extractString(request.children().get(0));
            if ("BLOCK".equalsIgnoreCase(command)) {
                return;
            }
            if ("SCAN".equalsIgnoreCase(command)) {
                ctx.writeAndFlush(RedisMessageHelper.array(
                        RedisMessageHelper.bulkString("7"),
                        RedisMessageHelper.array(
                                RedisMessageHelper.bulkString("user:1"),
                                RedisMessageHelper.bulkString("user:2"))));
                return;
            }
            String value = RedisMessageHelper.extractString(request.children().get(1));
            ctx.writeAndFlush(RedisMessageHelper.bulkString(value));
        }
    }
}
