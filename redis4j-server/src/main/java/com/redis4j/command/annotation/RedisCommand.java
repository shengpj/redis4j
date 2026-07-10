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
}
