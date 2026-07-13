package com.redis4j.server;

import com.redis4j.storage.StorageType;
import com.redis4j.persistence.aof.AofFlushPolicy;

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
                ", daemon=" + daemon +
                '}';
    }
}
