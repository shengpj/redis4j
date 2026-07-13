package com.redis4j.command;

import com.redis4j.protocol.response.CommandResponse;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** 写命令日志扩展点，由 AOF 等持久化机制实现。 */
public interface CommandJournal {
    boolean isWriteCommand(String commandName);

    CompletableFuture<Void> append(String commandName, String[] args, CommandResponse response) throws IOException;
}
