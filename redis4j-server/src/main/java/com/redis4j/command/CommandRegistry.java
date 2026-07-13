package com.redis4j.command;

import com.redis4j.command.impl.*;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.storage.DataStore;
import com.redis4j.storage.memory.MemoryManagedStore;
import com.redis4j.storage.memory.MemoryLimitManager;
import com.redis4j.storage.memory.WriteBackup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Callable;

/**
 * 命令注册表
 */
public class CommandRegistry {

    private static final Logger logger = LoggerFactory.getLogger(CommandRegistry.class);

    private final Map<String, Command> commands;
    private final DataStore dataStore;
    private final MemoryManagedStore managedStore;
    private final Object writeCommandLock = new Object();
    private volatile CommandJournal commandJournal;
    private volatile MemoryLimitManager memoryLimitManager;

    public CommandRegistry(DataStore dataStore) {
        this.commands = new HashMap<>();
        this.dataStore = dataStore;
        this.managedStore = dataStore instanceof MemoryManagedStore store ? store : null;
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
        MemoryLimitManager memoryManager = memoryLimitManager;
        boolean writeCommand = journal != null && journal.isWriteCommand(commandName)
                || memoryManager != null && memoryManager.isEnabled()
                && WriteCommandSupport.isWriteCommand(commandName);
        if (writeCommand) {
            synchronized (writeCommandLock) {
                // 在同一临界区内等待持久化最终结果，失败回滚时不会覆盖后续写命令。
                boolean journaledWrite = journal != null && journal.isWriteCommand(commandName);
                if (journaledWrite) {
                    try {
                        journal.ensureWritable();
                    } catch (Exception e) {
                        logger.error("Command journal is not writable: {}", commandName, e);
                        return persistenceError(e);
                    }
                }
                if (managedStore == null) {
                    logger.error("DataStore does not support rollback for write command: {}", commandName);
                    return CommandResponses.error("MISCONF DataStore does not support transactional rollback");
                }

                WriteBackup backup = captureWriteBackup(commandName, args);
                CommandResponse response = executeCommand(commandName, args);
                if (!(response instanceof CommandResponse.Error)) {
                    List<String> evictedKeys = List.of();
                    if (memoryManager != null) {
                        memoryManager.recordAccess(commandName, args);
                        MemoryLimitManager.EnforcementResult enforcement = memoryManager.enforce(commandName, backup);
                        if (!enforcement.accepted()) {
                            return CommandResponses.error("OOM command not allowed when used memory > 'maxmemory'");
                        }
                        evictedKeys = enforcement.evictedKeys();
                        backup = enforcement.backup();
                    }
                    if (journaledWrite) {
                        try {
                            CompletableFuture<Void> completion = journal.appendWithEvictions(
                                    commandName, args, response, evictedKeys);
                            Throwable failure = awaitCompletion(completion);
                            if (failure != null) {
                                logger.error("Failed to persist command: {}", commandName, failure);
                                return rollbackAfterPersistenceFailure(commandName, backup, failure);
                            }
                        } catch (Exception e) {
                            logger.error("Failed to append command to journal: {}", commandName, e);
                            return rollbackAfterPersistenceFailure(commandName, backup, e);
                        }
                    }
                }
                return response;
            }
        }
        CommandResponse response = executeCommand(commandName, args);
        if (memoryManager != null) memoryManager.recordAccess(commandName, args);
        return response;
    }

    private WriteBackup captureWriteBackup(String commandName, String[] args) {
        Set<String> keys = WriteCommandSupport.keys(commandName, args);
        if ("FLUSHDB".equalsIgnoreCase(commandName) || "FLUSHALL".equalsIgnoreCase(commandName)) {
            keys = dataStore.getAllKeys();
        }
        return WriteBackup.capture(managedStore, keys);
    }

    private CommandResponse rollbackAfterPersistenceFailure(String commandName, WriteBackup backup,
                                                              Throwable failure) {
        try {
            backup.restore(managedStore);
        } catch (Exception rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
            logger.error("Failed to rollback command after persistence failure: {}", commandName, rollbackFailure);
            return CommandResponses.error("MISCONF persistence and rollback both failed: " + messageOf(failure));
        }
        return persistenceError(failure);
    }

    private static Throwable awaitCompletion(CompletableFuture<Void> completion) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    completion.get();
                    return null;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    return e.getCause() == null ? e : e.getCause();
                } catch (CancellationException e) {
                    return e;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    private static CommandResponse persistenceError(Throwable failure) {
        return CommandResponses.error("MISCONF AOF persistence failed: " + messageOf(failure));
    }

    private static String messageOf(Throwable failure) {
        return failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
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

    public void setMemoryLimitManager(MemoryLimitManager memoryLimitManager) {
        synchronized (writeCommandLock) {
            this.memoryLimitManager = memoryLimitManager;
        }
    }

    /** 在与普通写命令相同的全局临界区内执行维护操作。 */
    public <T> T withWriteCommandLock(Callable<T> operation) throws Exception {
        synchronized (writeCommandLock) {
            return operation.call();
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
