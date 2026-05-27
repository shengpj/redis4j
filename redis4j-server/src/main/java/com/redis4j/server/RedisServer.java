package com.redis4j.server;

import com.redis4j.command.CommandRegistry;
import com.redis4j.command.impl.ServerCommands;
import com.redis4j.persistence.PersistenceManager;
import com.redis4j.storage.DataStore;
import com.redis4j.storage.DataStoreFactory;
import com.redis4j.storage.StorageType;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.redis.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Redis 服务端主类
 * 基于 Netty 实现高性能 TCP Server
 */
public class RedisServer {

    private static final Logger logger = LoggerFactory.getLogger(RedisServer.class);

    private final ServerConfig config;
    private final DataStore dataStore;
    private final CommandRegistry commandRegistry;
    private final PersistenceManager persistenceManager;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ThreadPoolExecutor commandExecutor;
    private Channel serverChannel;

    public RedisServer() {
        this(ServerConfig.defaultConfig());
    }

    public RedisServer(ServerConfig config) {
        this.config = config;
        this.dataStore = DataStoreFactory.create(config.getDataStoreType(), config.getPartitions());
        this.commandRegistry = new CommandRegistry(dataStore);
        this.persistenceManager = new PersistenceManager(dataStore, config.getDataDir());
        logger.info("Using DataStore type: {}", config.getDataStoreType());
    }

    public RedisServer(DataStore dataStore, CommandRegistry commandRegistry, ServerConfig config) {
        this.config = config;
        this.dataStore = dataStore;
        this.commandRegistry = commandRegistry;
        this.persistenceManager = new PersistenceManager(dataStore, config.getDataDir());
    }

    /**
     * 启动服务器
     */
    public void start() throws InterruptedException {
        logger.info("Starting Redis4J server on {}:{}", config.getHost(), config.getPort());

        // 启动时加载 RDB 数据
        persistenceManager.load();

        // Boss 线程固定为 1，因为只有一个 ServerSocketChannel 接受连接
        bossGroup = new NioEventLoopGroup(1);
        // Worker 线程用于 I/O 操作
        workerGroup = new NioEventLoopGroup(config.getWorkerThreads());
        // 命令处理线程池，独立于 Netty 线程
        // 使用有界队列避免无限制堆叠
        AtomicInteger threadNum = new AtomicInteger(1);
        commandExecutor = new ThreadPoolExecutor(
                config.getWorkerThreads(),
                config.getWorkerThreads(),
                0L, TimeUnit.MILLISECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(1024),
                r -> {
                    Thread t = new Thread(r, "redis-cmd-" + threadNum.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
        );

        // 注册持久化命令
        commandRegistry.register(new ServerCommands.SaveCommand(persistenceManager));
        commandRegistry.register(new ServerCommands.BgSaveCommand(persistenceManager));
        commandRegistry.register(new ServerCommands.LastSaveCommand(persistenceManager));

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();

                        // 使用 Netty codec-redis
                        pipeline.addLast(new RedisDecoder());
                        pipeline.addLast(new RedisMessageAggregator());
                        pipeline.addLast(new RedisEncoder());
                        pipeline.addLast(new NettyCodecHandler(commandRegistry, commandExecutor));
                    }
                });

        ChannelFuture future = bootstrap.bind(config.getHost(), config.getPort());
        future.sync();

        serverChannel = future.channel();
        logger.info("Redis4J server started successfully on port {}", config.getPort());

        // 启动后台定时 RDB 保存
        persistenceManager.start();

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        serverChannel.closeFuture().sync();
    }

    /**
     * 关闭服务器
     */
    public void shutdown() {
        logger.info("Shutting down Redis4J server...");

        // 停止定时任务
        persistenceManager.stop();

        // 关闭前最后一次保存
        logger.info("Saving RDB before shutdown...");
        persistenceManager.save();
        logger.info("RDB save completed");

        if (serverChannel != null) {
            serverChannel.close();
        }

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }

        if (commandExecutor != null) {
            commandExecutor.shutdown();
            try {
                if (!commandExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    commandExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                commandExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (dataStore != null) {
            dataStore.close();
        }

        logger.info("Redis4J server stopped");
    }

    public DataStore getDataStore() {
        return dataStore;
    }

    public CommandRegistry getCommandRegistry() {
        return commandRegistry;
    }

    public PersistenceManager getPersistenceManager() {
        return persistenceManager;
    }

    public ServerConfig getConfig() {
        return config;
    }

    public static void main(String[] args) {
        ServerConfig config = ServerConfig.defaultConfig();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-p", "--port" -> {
                    if (i + 1 < args.length) {
                        config.setPort(Integer.parseInt(args[++i]));
                    }
                }
                case "-h", "--host" -> {
                    if (i + 1 < args.length) {
                        config.setHost(args[++i]);
                    }
                }
                case "--daemon" -> config.setDaemon(true);
                case "--store", "--datastore" -> {
                    if (i + 1 < args.length) {
                        String type = args[++i].toUpperCase();
                        config.setDataStoreType(StorageType.valueOf(type));
                    }
                }
            }
        }

        if (config.isDaemon()) {
            logger.info("Daemon mode not fully implemented, running in foreground");
        }

        try {
            RedisServer server = new RedisServer(config);
            server.start();
        } catch (Exception e) {
            logger.error("Failed to start server", e);
            System.exit(1);
        }
    }
}
