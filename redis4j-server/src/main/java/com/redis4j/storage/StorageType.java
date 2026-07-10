package com.redis4j.storage;

/**
 * 存储类型枚举
 */
public enum StorageType {
    /**
     * 基础内存存储
     */
    MEMORY,

    /**
     * 分区内存存储（多线程优化）
     */
    PARTITIONED
}
