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

/**
 * 命令注册表
 */
public class CommandRegistry {

    private static final Logger logger = LoggerFactory.getLogger(CommandRegistry.class);

    private final Map<String, Command> commands;
    private final DataStore dataStore;

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
