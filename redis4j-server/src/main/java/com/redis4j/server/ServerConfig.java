package com.redis4j.server;

/**
 * Redis 服务端配置
 */
public class ServerConfig {

    public enum DataStoreType {
        MEMORY,      // JDK ConcurrentHashMap
        PARTITIONED, // 分区 ConcurrentHashMap (高并发推荐)
        AGRONA,      // Agrona 高性能数据结构 (已废弃)
        ECLIPSE      // Eclipse Collections (已废弃)
    }

    private int partitions = Runtime.getRuntime().availableProcessors() * 2; // 分区数量

    private int port = 6379;
    private String host = "0.0.0.0";
    private int workerThreads = 8;
    private int maxFrameLength = 1024 * 1024;
    private int soTimeout = 0;
    private String dataDir = "./data";
    private boolean daemon = false;
    private DataStoreType dataStoreType = DataStoreType.PARTITIONED;

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

    public DataStoreType getDataStoreType() {
        return dataStoreType;
    }

    public void setDataStoreType(DataStoreType dataStoreType) {
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
        this.maxFrameLength = maxFrameLength;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
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
                ", soTimeout=" + soTimeout +
                ", dataDir='" + dataDir + '\'' +
                ", daemon=" + daemon +
                '}';
    }
}
