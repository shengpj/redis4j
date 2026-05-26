package com.redis4j.storage;

import com.redis4j.storage.type.RedisValue;

import java.util.Map;
import java.util.Set;

/**
 * 数据类型枚举
 */
public enum DataType {
    STRING("string"),
    LIST("list"),
    HASH("hash"),
    SET("set"),
    ZSET("zset"),
    NONE("none");

    private final String name;

    DataType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static DataType fromString(String name) {
        for (DataType type : values()) {
            if (type.name.equalsIgnoreCase(name)) {
                return type;
            }
        }
        return NONE;
    }
}
