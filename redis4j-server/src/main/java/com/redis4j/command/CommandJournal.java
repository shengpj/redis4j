package com.redis4j.command;

import com.redis4j.protocol.response.CommandResponse;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.List;

/** 写命令日志扩展点，由 AOF 等持久化机制实现。 */
public interface CommandJournal {
    boolean isWriteCommand(String commandName);

    default void ensureWritable() throws IOException {
    }

    CompletableFuture<Void> append(String commandName, String[] args, CommandResponse response) throws IOException;

    default CompletableFuture<Void> appendWithEvictions(String commandName, String[] args,
                                                          CommandResponse response, List<String> evictedKeys)
            throws IOException {
        if (!evictedKeys.isEmpty()) {
            throw new IOException("Command journal does not support atomic eviction persistence");
        }
        return append(commandName, args, response);
    }
}
