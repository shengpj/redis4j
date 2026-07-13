package com.redis4j.server;

import com.redis4j.storage.StorageType;
import com.redis4j.persistence.aof.AofFlushPolicy;
import com.redis4j.storage.memory.EvictionPolicy;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Redis 服务端配置
 */
public class ServerConfig {

    private int partitions = Runtime.getRuntime().availableProcessors() * 2; // 分区数量

    private int port = 6379;
    private String host = "0.0.0.0";
    private int workerThreads = 8;
    private int maxFrameLength = 1024 * 1024;
    private int maxArrayLength = 1024;
    private int maxPendingCommandsPerConnection = 1024;
    private int commandQueueCapacity = 1024;
    private String dataDir = "./data";
    private boolean appendOnly = false;
    private AofFlushPolicy appendFsync = AofFlushPolicy.EVERYSEC;
    private String appendFilename = "appendonly.aof";
    private int aofQueueCapacity = 8192;
    private boolean aofUseRdbPreamble = true;
    private long autoAofRewriteMinSize = 64L * 1024 * 1024;
    private int autoAofRewritePercentage = 100;
    private long maxMemoryBytes;
    private EvictionPolicy maxMemoryPolicy = EvictionPolicy.NOEVICTION;
    private long slowLogSlowerThanMicros = 10_000;
    private int slowLogMaxLen = 128;
    private boolean daemon = false;
    private StorageType dataStoreType = StorageType.PARTITIONED;

    public ServerConfig() {
    }

    public ServerConfig(int port) {
        this.port = port;
    }

    public static ServerConfig defaultConfig() {
        return new ServerConfig();
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public StorageType getDataStoreType() {
        return dataStoreType;
    }

    public void setDataStoreType(StorageType dataStoreType) {
        this.dataStoreType = dataStoreType;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public int getMaxFrameLength() {
        return maxFrameLength;
    }

    public void setMaxFrameLength(int maxFrameLength) {
        if (maxFrameLength <= 0) throw new IllegalArgumentException("maxFrameLength must be positive");
        if (maxFrameLength > 512 * 1024 * 1024) {
            throw new IllegalArgumentException("maxFrameLength cannot exceed Netty's 512 MiB RESP limit");
        }
        this.maxFrameLength = maxFrameLength;
    }

    public int getMaxArrayLength() {
        return maxArrayLength;
    }

    public void setMaxArrayLength(int maxArrayLength) {
        if (maxArrayLength <= 0) throw new IllegalArgumentException("maxArrayLength must be positive");
        this.maxArrayLength = maxArrayLength;
    }

    public int getMaxPendingCommandsPerConnection() {
        return maxPendingCommandsPerConnection;
    }

    public void setMaxPendingCommandsPerConnection(int value) {
        if (value <= 0) throw new IllegalArgumentException("maxPendingCommandsPerConnection must be positive");
        this.maxPendingCommandsPerConnection = value;
    }

    public int getCommandQueueCapacity() {
        return commandQueueCapacity;
    }

    public void setCommandQueueCapacity(int commandQueueCapacity) {
        if (commandQueueCapacity <= 0) throw new IllegalArgumentException("commandQueueCapacity must be positive");
        this.commandQueueCapacity = commandQueueCapacity;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public boolean isAppendOnly() {
        return appendOnly;
    }

    public void setAppendOnly(boolean appendOnly) {
        this.appendOnly = appendOnly;
    }

    public AofFlushPolicy getAppendFsync() {
        return appendFsync;
    }

    public void setAppendFsync(AofFlushPolicy appendFsync) {
        this.appendFsync = java.util.Objects.requireNonNull(appendFsync, "appendFsync");
    }

    public String getAppendFilename() {
        return appendFilename;
    }

    public void setAppendFilename(String appendFilename) {
        if (appendFilename == null || appendFilename.isBlank()) {
            throw new IllegalArgumentException("appendFilename cannot be blank");
        }
        this.appendFilename = appendFilename;
    }

    public int getAofQueueCapacity() {
        return aofQueueCapacity;
    }

    public void setAofQueueCapacity(int aofQueueCapacity) {
        if (aofQueueCapacity <= 0) throw new IllegalArgumentException("aofQueueCapacity must be positive");
        this.aofQueueCapacity = aofQueueCapacity;
    }

    public boolean isAofUseRdbPreamble() {
        return aofUseRdbPreamble;
    }

    public void setAofUseRdbPreamble(boolean aofUseRdbPreamble) {
        this.aofUseRdbPreamble = aofUseRdbPreamble;
    }

    public long getAutoAofRewriteMinSize() {
        return autoAofRewriteMinSize;
    }

    public void setAutoAofRewriteMinSize(long autoAofRewriteMinSize) {
        if (autoAofRewriteMinSize < 0) throw new IllegalArgumentException("autoAofRewriteMinSize cannot be negative");
        this.autoAofRewriteMinSize = autoAofRewriteMinSize;
    }

    public int getAutoAofRewritePercentage() {
        return autoAofRewritePercentage;
    }

    public void setAutoAofRewritePercentage(int autoAofRewritePercentage) {
        if (autoAofRewritePercentage < 0) {
            throw new IllegalArgumentException("autoAofRewritePercentage cannot be negative");
        }
        this.autoAofRewritePercentage = autoAofRewritePercentage;
    }

    public long getMaxMemoryBytes() {
        return maxMemoryBytes;
    }

    public void setMaxMemoryBytes(long maxMemoryBytes) {
        if (maxMemoryBytes < 0) throw new IllegalArgumentException("maxMemoryBytes cannot be negative");
        this.maxMemoryBytes = maxMemoryBytes;
    }

    public EvictionPolicy getMaxMemoryPolicy() {
        return maxMemoryPolicy;
    }

    public void setMaxMemoryPolicy(EvictionPolicy maxMemoryPolicy) {
        if (maxMemoryPolicy == null) throw new IllegalArgumentException("maxMemoryPolicy is required");
        this.maxMemoryPolicy = maxMemoryPolicy;
    }

    public long getSlowLogSlowerThanMicros() {
        return slowLogSlowerThanMicros;
    }

    public void setSlowLogSlowerThanMicros(long slowLogSlowerThanMicros) {
        if (slowLogSlowerThanMicros < -1)
            throw new IllegalArgumentException("slowLogSlowerThanMicros cannot be less than -1");
        this.slowLogSlowerThanMicros = slowLogSlowerThanMicros;
    }

    public int getSlowLogMaxLen() {
        return slowLogMaxLen;
    }

    public void setSlowLogMaxLen(int slowLogMaxLen) {
        if (slowLogMaxLen < 0) throw new IllegalArgumentException("slowLogMaxLen cannot be negative");
        this.slowLogMaxLen = slowLogMaxLen;
    }

    public Map<String, String> asConfigMap() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("bind", host);
        values.put("port", Integer.toString(port));
        values.put("worker-threads", Integer.toString(workerThreads));
        values.put("datastore", dataStoreType.name().toLowerCase(Locale.ROOT));
        values.put("partitions", Integer.toString(partitions));
        values.put("max-frame-length", Integer.toString(maxFrameLength));
        values.put("max-array-length", Integer.toString(maxArrayLength));
        values.put("max-pending-commands-per-connection", Integer.toString(maxPendingCommandsPerConnection));
        values.put("command-queue-capacity", Integer.toString(commandQueueCapacity));
        values.put("dir", dataDir);
        values.put("appendonly", yesNo(appendOnly));
        values.put("appendfsync", appendFsync.name().toLowerCase(Locale.ROOT));
        values.put("appendfilename", appendFilename);
        values.put("aof-queue-capacity", Integer.toString(aofQueueCapacity));
        values.put("aof-use-rdb-preamble", yesNo(aofUseRdbPreamble));
        values.put("auto-aof-rewrite-min-size", Long.toString(autoAofRewriteMinSize));
        values.put("auto-aof-rewrite-percentage", Integer.toString(autoAofRewritePercentage));
        values.put("maxmemory", Long.toString(maxMemoryBytes));
        values.put("maxmemory-policy", maxMemoryPolicy.name().toLowerCase(Locale.ROOT).replace('_', '-'));
        values.put("slowlog-log-slower-than", Long.toString(slowLogSlowerThanMicros));
        values.put("slowlog-max-len", Integer.toString(slowLogMaxLen));
        values.put("daemonize", yesNo(daemon));
        return Collections.unmodifiableMap(values);
    }

    private static String yesNo(boolean value) {
        return value ? "yes" : "no";
    }

    public boolean isDaemon() {
        return daemon;
    }

    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    @Override
    public String toString() {
        return "ServerConfig{" +
                "port=" + port +
                ", host='" + host + '\'' +
                ", workerThreads=" + workerThreads +
                ", dataStoreType=" + dataStoreType +
                ", partitions=" + partitions +
                ", maxFrameLength=" + maxFrameLength +
                ", maxArrayLength=" + maxArrayLength +
                ", maxPendingCommandsPerConnection=" + maxPendingCommandsPerConnection +
                ", commandQueueCapacity=" + commandQueueCapacity +
                ", dataDir='" + dataDir + '\'' +
                ", appendOnly=" + appendOnly +
                ", appendFsync=" + appendFsync +
                ", appendFilename='" + appendFilename + '\'' +
                ", aofQueueCapacity=" + aofQueueCapacity +
                ", aofUseRdbPreamble=" + aofUseRdbPreamble +
                ", autoAofRewriteMinSize=" + autoAofRewriteMinSize +
                ", autoAofRewritePercentage=" + autoAofRewritePercentage +
                ", maxMemoryBytes=" + maxMemoryBytes +
                ", maxMemoryPolicy=" + maxMemoryPolicy +
                ", slowLogSlowerThanMicros=" + slowLogSlowerThanMicros +
                ", slowLogMaxLen=" + slowLogMaxLen +
                ", daemon=" + daemon +
                '}';
    }
}
