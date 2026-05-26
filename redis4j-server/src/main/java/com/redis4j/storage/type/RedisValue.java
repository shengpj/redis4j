package com.redis4j.storage.type;

import com.redis4j.storage.DataType;

/**
 * Redis 值接口
 */
public interface RedisValue {

    /**
     * 获取数据类型
     */
    DataType getType();

    /**
     * 获取原始值对象
     */
    Object getValue();

    /**
     * 是否已过期
     */
    boolean isExpired();

    /**
     * 获取剩余生存时间（毫秒）
     */
    long getTtl();
}
