package com.redis4j.storage;

/**
 * DataStore 工厂类
 */
public final class DataStoreFactory {

    private DataStoreFactory() {
    }

    /**
     * 根据类型创建 DataStore 实例
     */
    public static DataStore create(StorageType type) {
        return create(type, null);
    }

    /**
     * 根据类型创建 DataStore 实例
     * @param type 存储类型
     * @param partitionCount 分区数量（仅 PARTITIONED 类型有效）
     */
    public static DataStore create(StorageType type, Integer partitionCount) {
        switch (type) {
            case MEMORY:
                return new MemoryStore();

            case PARTITIONED:
                int partitions = partitionCount != null ? partitionCount : Runtime.getRuntime().availableProcessors();
                return new PartitionedMemoryStore(partitions);

            case ECLIPSE_COLLECTIONS:
                return new EclipseCollectionsStore();

            case CAFFEINE:
                int max =  10000;
                return new CaffeineStore(max);

            default:
                throw new IllegalArgumentException("Unknown storage type: " + type);
        }
    }

    /**
     * 根据字符串创建 DataStore 实例
     */
    public static DataStore create(String typeName) {
        return create(typeName, null);
    }

    /**
     * 根据字符串创建 DataStore 实例
     */
    public static DataStore create(String typeName, Integer partitionCount) {
        StorageType type;
        try {
            type = StorageType.valueOf(typeName.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown storage type: " + typeName +
                ". Available types: MEMORY, PARTITIONED, ECLIPSE_COLLECTIONS");
        }
        return create(type, partitionCount);
    }
}
