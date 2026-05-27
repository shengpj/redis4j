package com.redis4j.command.annotation;

import java.lang.annotation.*;

/**
 * 命令注解
 * 标记一个类为 Redis 命令
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RedisCommand {

    /**
     * 命令名称，如 "GET", "SET"
     */
    String name();

    /**
     * 命令参数数量
     * 正数表示固定参数数量
     * -1 表示至少 1 个参数
     * -2 表示至少 2 个参数
     */
    int arity() default 2;
}
