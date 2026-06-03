package com.redis4j.client;

import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.protocol.RedisMessageUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.redis.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Redis 客户端
 * 使用 Netty codec-redis 模块
 */
public class RedisClient {

    private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);

    private final String host;
    private final int port;

    private EventLoopGroup group;
    private Channel channel;
    private volatile boolean connected = false;

    private final List<Object> pendingResponses = new ArrayList<>();
    private final Object responseLock = new Object();

    public RedisClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public RedisClient(String host) {
        this(host, 6379);
    }

    public RedisClient() {
        this("localhost", 6379);
    }

    /**
     * 连接到服务端
     */
    public void connect() throws InterruptedException {
        if (connected) {
            return;
        }

        group = new NioEventLoopGroup();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();

                        // 使用 Netty codec-redis
                        pipeline.addLast(new RedisDecoder());
                        pipeline.addLast(new RedisBulkStringAggregator()); // 客户端也需要聚合 bulk string
                        pipeline.addLast(new RedisEncoder());
                        pipeline.addLast(new SimpleChannelInboundHandler<RedisMessage>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, RedisMessage msg) throws Exception {
                                // 服务端心跳 PING → 自动回复 PONG
                                if (msg instanceof SimpleStringRedisMessage s && "PING".equalsIgnoreCase(s.content())) {
                                    ctx.writeAndFlush(RedisMessageHelper.simpleString("PONG"));
                                    return;
                                }
                                RedisMessage copy = RedisMessageUtil.deepCopy(msg);
                                synchronized (responseLock) {
                                    pendingResponses.add(copy);
                                    responseLock.notifyAll();
                                }
                            }
                        });
                    }
                });

        ChannelFuture future = bootstrap.connect(host, port);
        future.sync();

        channel = future.channel();
        connected = true;
        logger.info("Connected to Redis server at {}:{}", host, port);

        channel.closeFuture().addListener(f -> {
            connected = false;
            logger.info("Disconnected from Redis server");
        });
    }

    /**
     * 断开连接
     */
    public void disconnect() {
        if (channel != null) {
            channel.close();
        }
        if (group != null) {
            group.shutdownGracefully();
        }
        connected = false;
    }

    /**
     * 发送命令并获取响应
     */
    public RedisMessage sendCommand(String... args) throws InterruptedException {
        if (!connected || channel == null) {
            throw new IllegalStateException("Not connected to Redis server");
        }

        // 构建命令数组
        RedisMessage request = buildArrayMessage(args);

        // 清空之前的响应
        synchronized (responseLock) {
            pendingResponses.clear();
        }

        // 发送请求
        channel.writeAndFlush(request).sync();

        // 等待响应
        long deadline = System.currentTimeMillis() + 30000;
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            synchronized (responseLock) {
                if (!pendingResponses.isEmpty()) {
                    return (RedisMessage) pendingResponses.remove(0);
                }
                responseLock.wait(remaining);
            }
        }

        return null;
    }

    /**
     * 异步发送命令
     */
    public void sendCommandAsync(Consumer<RedisMessage> callback, String... args) {
        if (!connected || channel == null) {
            throw new IllegalStateException("Not connected to Redis server");
        }

        RedisMessage request = buildArrayMessage(args);
        channel.writeAndFlush(request).addListener(future -> {
            if (!future.isSuccess()) {
                callback.accept(RedisMessageHelper.error("ERR", "send failed"));
            }
        });

        // 异步等待响应
        Thread responseThread = new Thread(() -> {
            long deadline = System.currentTimeMillis() + 30000;
            while (true) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    callback.accept(RedisMessageHelper.error("ERR", "timeout"));
                    return;
                }
                synchronized (responseLock) {
                    if (!pendingResponses.isEmpty()) {
                        callback.accept((RedisMessage) pendingResponses.remove(0));
                        return;
                    }
                    try {
                        responseLock.wait(remaining);
                    } catch (InterruptedException e) {
                        callback.accept(RedisMessageHelper.error("ERR", "interrupted"));
                        return;
                    }
                }
            }
        });
        responseThread.start();
    }

    private RedisMessage buildArrayMessage(String... args) {
        List<RedisMessage> items = new ArrayList<>(args.length);
        for (String arg : args) {
            items.add(RedisMessageHelper.bulkString(arg));
        }
        return new ArrayRedisMessage(items);
    }

    public boolean isConnected() {
        return connected && channel != null && channel.isActive();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
