package com.redis4j.command;

import com.redis4j.command.impl.*;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.storage.DataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 命令注册表
 */
public class CommandRegistry {

    private static final Logger logger = LoggerFactory.getLogger(CommandRegistry.class);

    private final Map<String, Command> commands;
    private final DataStore dataStore;
    private final Object writeCommandLock = new Object();
    private volatile CommandJournal commandJournal;

    public CommandRegistry(DataStore dataStore) {
        this.commands = new HashMap<>();
        this.dataStore = dataStore;
        registerDefaultCommands();
    }

    /**
     * 注册命令
     */
    public void register(Command command) {
        commands.put(command.metadata().name(), command);
    }

    /**
     * 查找命令
     */
    public Command find(String name) {
        return commands.get(name.toUpperCase());
    }

    /**
     * 执行命令
     */
    public CommandResponse execute(String commandName, String[] args) {
        CommandJournal journal = commandJournal;
        if (journal != null && journal.isWriteCommand(commandName)) {
            CommandResponse response;
            CompletableFuture<Void> journalCompletion = null;
            // 锁内完成数据修改和按序入队；磁盘等待移到锁外，使单写线程能够聚合多个连接的记录。
            synchronized (writeCommandLock) {
                response = executeCommand(commandName, args);
                if (!(response instanceof CommandResponse.Error)) {
                    try {
                        journalCompletion = journal.append(commandName, args, response);
                    } catch (Exception e) {
                        logger.error("Failed to append command to journal: {}", commandName, e);
                        return CommandResponses.error("MISCONF AOF persistence failed: " + e.getMessage());
                    }
                }
            }
            if (journalCompletion != null) {
                try {
                    journalCompletion.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return CommandResponses.error("MISCONF interrupted while waiting for AOF persistence");
                } catch (ExecutionException e) {
                    logger.error("Failed to persist AOF command: {}", commandName, e.getCause());
                    return CommandResponses.error("MISCONF AOF persistence failed: " + e.getCause().getMessage());
                }
            }
            return response;
        }
        return executeCommand(commandName, args);
    }

    /** AOF 启动恢复专用入口，重放时不能再次写入 AOF。 */
    public CommandResponse executeReplay(String commandName, String[] args) {
        return executeCommand(commandName, args);
    }

    private CommandResponse executeCommand(String commandName, String[] args) {
        Command command = find(commandName);
        if (command == null) {
            return CommandResponses.error("ERR unknown command '" + commandName + "'");
        }

        try {
            return command.execute(args);
        } catch (Exception e) {
            logger.error("Error executing command: {}", commandName, e);
            return CommandResponses.error("ERR " + e.getMessage());
        }
    }

    public void setCommandJournal(CommandJournal commandJournal) {
        synchronized (writeCommandLock) {
            this.commandJournal = commandJournal;
        }
    }

    /**
     * 获取所有已注册的命令名称
     */
    public Set<String> getCommandNames() {
        return commands.keySet();
    }

    /**
     * 注册默认命令
     */
    private void registerDefaultCommands() {
        // 使用扫描器自动注册命令
        CommandScanner scanner = new CommandScanner(dataStore);
        Map<String, Command> scanned = scanner.scanAndRegister(
            "com.redis4j.command.impl"
        );
        commands.putAll(scanned);

        logger.info("Registered {} commands", commands.size());
    }
}
