package com.redis4j.command;

import io.netty.handler.codec.redis.RedisMessage;

/**
 * 命令接口
 */
public interface Command {

    /**
     * 获取命令名称
     */
    String getName();

    /**
     * 获取命令参数数量（-1 表示可变参数）
     */
    int getArity();

    /**
     * 执行命令
     * @param args 命令参数（不包括命令名称本身）
     * @return 响应
     */
    RedisMessage execute(String[] args);
}
