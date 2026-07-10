package com.redis4j.command;

import com.redis4j.protocol.response.CommandResponse;

/**
 * 命令接口
 */
public interface Command {

    default CommandMetadata metadata() {
        return new CommandMetadata(getName(), Arity.fromLegacy(getArity()));
    }

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
    CommandResponse execute(String[] args);
}
