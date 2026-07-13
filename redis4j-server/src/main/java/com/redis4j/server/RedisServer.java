package com.redis4j.server;

import com.redis4j.command.CommandRegistry;
import com.redis4j.command.impl.ServerCommands;
import com.redis4j.persistence.PersistenceManager;
import com.redis4j.persistence.aof.AofFlushPolicy;
import com.redis4j.persistence.aof.AofManager;
import com.redis4j.storage.DataStore;
import com.redis4j.storage.DataStoreFactory;
import com.redis4j.storage.StorageType;
import com.redis4j.storage.memory.EvictionPolicy;
import com.redis4j.storage.memory.MemoryLimitManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.redis.*;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;

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
    private final AofManager aofManager;
    private final PubSubBroker pubSubBroker = new PubSubBroker();
    private final ServerObservability observability;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ThreadPoolExecutor commandExecutor;
    private ScheduledExecutorService aofMaintenanceExecutor;
    private Channel serverChannel;
    private final AtomicBoolean shuttingDown = new AtomicBoolean();

    public RedisServer() {
        this(ServerConfig.defaultConfig());
    }

    public RedisServer(ServerConfig config) {
        this.config = config;
        this.dataStore = DataStoreFactory.create(config.getDataStoreType(), config.getPartitions());
        this.commandRegistry = new CommandRegistry(dataStore);
        this.commandRegistry.setMemoryLimitManager(new MemoryLimitManager(dataStore,
                config.getMaxMemoryBytes(), config.getMaxMemoryPolicy()));
        this.persistenceManager = new PersistenceManager(dataStore, config.getDataDir());
        this.aofManager = createAofManager(config);
        this.observability = new ServerObservability(config);
        logger.info("Using DataStore type: {}", config.getDataStoreType());
    }

    public RedisServer(DataStore dataStore, CommandRegistry commandRegistry, ServerConfig config) {
        this.config = config;
        this.dataStore = dataStore;
        this.commandRegistry = commandRegistry;
        this.commandRegistry.setMemoryLimitManager(new MemoryLimitManager(dataStore,
                config.getMaxMemoryBytes(), config.getMaxMemoryPolicy()));
        this.persistenceManager = new PersistenceManager(dataStore, config.getDataDir());
        this.aofManager = createAofManager(config);
        this.observability = new ServerObservability(config);
    }

    /**
     * 启动服务器
     */
    public void start() throws InterruptedException {
        logger.info("Effective Redis4J server configuration: {}, aofPath={}",
                config, aofManager == null ? "disabled" : aofManager.getPath());
        logger.info("Starting Redis4J server on {}:{}", config.getHost(), config.getPort());

        initializePersistence();

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
                new java.util.concurrent.LinkedBlockingQueue<>(config.getCommandQueueCapacity()),
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
        commandRegistry.register(new ServerCommands.InfoCommand(dataStore, config));
        if (aofManager != null) {
            commandRegistry.register(new ServerCommands.BgRewriteAofCommand(aofManager, commandRegistry, dataStore));
        }

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

                        // 空闲超时：60 秒无读事件自动关连，防止半连接占用资源
                        pipeline.addLast(new IdleStateHandler(30, 0, 0, TimeUnit.SECONDS));

                        // 使用 Netty codec-redis
                        pipeline.addLast(new RedisDecoder(config.getMaxFrameLength(), FixedRedisMessagePool.INSTANCE));
                        pipeline.addLast(new RedisMessageSizeLimiter(config.getMaxFrameLength()));
                        pipeline.addLast(new RedisBulkStringAggregator());
                        pipeline.addLast(new RedisMessageAggregator(config.getMaxArrayLength()));
                        pipeline.addLast(new RedisEncoder());
                        pipeline.addLast(new FlushConsolidationHandler(256, true));
                        pipeline.addLast(new NettyCodecHandler(commandRegistry, commandExecutor, pubSubBroker,
                                observability, config.getMaxPendingCommandsPerConnection()));
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
        if (!shuttingDown.compareAndSet(false, true)) {
            return;
        }
        logger.info("Shutting down Redis4J server...");

        // 停止定时任务
        persistenceManager.stop();
        if (aofMaintenanceExecutor != null) {
            aofMaintenanceExecutor.shutdownNow();
        }

        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
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

        // 所有写命令处理完毕后再关闭 AOF，确保队列中的记录全部顺序写入并刷盘。
        if (aofManager != null) {
            commandRegistry.setCommandJournal(null);
            aofManager.close();
        }

        // 命令线程和 AOF 都已停止，此时生成的最终 RDB 是稳定的关闭快照。
        logger.info("Saving RDB before shutdown...");
        persistenceManager.save();
        logger.info("RDB save completed");

        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        }

        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
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

    public AofManager getAofManager() {
        return aofManager;
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
                case "--appendonly" -> config.setAppendOnly(true);
                case "--appendfsync" -> {
                    if (i + 1 < args.length) config.setAppendFsync(AofFlushPolicy.parse(args[++i]));
                }
                case "--aof-use-rdb-preamble" -> {
                    if (i + 1 < args.length) config.setAofUseRdbPreamble(Boolean.parseBoolean(args[++i]));
                }
                case "--maxmemory" -> {
                    if (i + 1 < args.length) config.setMaxMemoryBytes(parseMemorySize(args[++i]));
                }
                case "--maxmemory-policy" -> {
                    if (i + 1 < args.length) config.setMaxMemoryPolicy(EvictionPolicy.parse(args[++i]));
                }
                case "--slowlog-log-slower-than" -> {
                    if (i + 1 < args.length) config.setSlowLogSlowerThanMicros(Long.parseLong(args[++i]));
                }
                case "--slowlog-max-len" -> {
                    if (i + 1 < args.length) config.setSlowLogMaxLen(Integer.parseInt(args[++i]));
                }
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

    private void initializePersistence() {
        if (aofManager == null) {
            persistenceManager.load();
            return;
        }
        boolean aofExisted = aofManager.exists();
        try {
            if (aofExisted) {
                aofManager.recover(commandRegistry, dataStore);
            } else {
                // 第一次启用 AOF 时先继承现有 RDB 数据，再写入一份 AOF 基线。
                persistenceManager.load();
            }
            aofManager.start();
            if (!aofExisted && dataStore.dbSize() > 0) {
                aofManager.appendSnapshot(dataStore.createSnapshot());
            }
            commandRegistry.setCommandJournal(aofManager);
            startAofMaintenance();
            logger.info("AOF enabled: file={}, appendfsync={}", aofManager.getPath(), aofManager.getFlushPolicy());
        } catch (IOException e) {
            aofManager.close();
            throw new IllegalStateException("Failed to initialize AOF persistence", e);
        }
    }

    private static AofManager createAofManager(ServerConfig config) {
        if (!config.isAppendOnly()) return null;
        Path path = Path.of(config.getDataDir(), config.getAppendFilename());
        return new AofManager(path, config.getAppendFsync(), config.getAofQueueCapacity(),
                config.isAofUseRdbPreamble());
    }

    private void startAofMaintenance() {
        if (config.getAutoAofRewritePercentage() == 0) return;
        aofMaintenanceExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "aof-maintenance");
            thread.setDaemon(true);
            return thread;
        });
        aofMaintenanceExecutor.scheduleWithFixedDelay(() -> {
            if (aofManager.shouldAutoRewrite(config.getAutoAofRewriteMinSize(),
                    config.getAutoAofRewritePercentage())) {
                aofManager.bgRewrite(commandRegistry, dataStore);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private static long parseMemorySize(String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        long multiplier = 1;
        if (normalized.endsWith("kb")) {
            multiplier = 1024L;
            normalized = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("mb")) {
            multiplier = 1024L * 1024;
            normalized = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("gb")) {
            multiplier = 1024L * 1024 * 1024;
            normalized = normalized.substring(0, normalized.length() - 2);
        }
        return Math.multiplyExact(Long.parseLong(normalized), multiplier);
    }
}
