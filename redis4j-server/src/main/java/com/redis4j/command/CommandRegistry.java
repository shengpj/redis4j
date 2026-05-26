package com.redis4j.command;

import com.redis4j.command.impl.*;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.storage.DataStore;
import io.netty.handler.codec.redis.RedisMessage;
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
        commands.put(command.getName().toUpperCase(), command);
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
    public RedisMessage execute(String commandName, String[] args) {
        Command command = find(commandName);
        if (command == null) {
            return RedisMessageHelper.error("ERR", "unknown command '" + commandName + "'");
        }

        try {
            return command.execute(args);
        } catch (Exception e) {
            logger.error("Error executing command: {}", commandName, e);
            return RedisMessageHelper.error("ERR", e.getMessage());
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
        // String 命令
        register(new StringCommands.StringGetCommand(dataStore));
        register(new StringCommands.StringSetCommand(dataStore));
        register(new StringCommands.StringSetNxCommand(dataStore));
        register(new StringCommands.StringSetExCommand(dataStore));
        register(new StringCommands.StringMGetCommand(dataStore));
        register(new StringCommands.StringMSetCommand(dataStore));
        register(new StringCommands.StringIncrCommand(dataStore));
        register(new StringCommands.StringIncrByCommand(dataStore));
        register(new StringCommands.StringDecrCommand(dataStore));
        register(new StringCommands.StringDecrByCommand(dataStore));
        register(new StringCommands.StringStrlenCommand(dataStore));
        register(new StringCommands.StringAppendCommand(dataStore));

        // Key 命令
        register(new KeyCommands.KeyDelCommand(dataStore));
        register(new KeyCommands.KeyExistsCommand(dataStore));
        register(new KeyCommands.KeyExpireCommand(dataStore));
        register(new KeyCommands.KeyExpireAtCommand(dataStore));
        register(new KeyCommands.KeyTtlCommand(dataStore));
        register(new KeyCommands.KeyPttlCommand(dataStore));
        register(new KeyCommands.KeyPersistCommand(dataStore));
        register(new KeyCommands.KeyRenameCommand(dataStore));
        register(new KeyCommands.KeyTypeCommand(dataStore));
        register(new KeyCommands.KeyKeysCommand(dataStore));
        register(new KeyCommands.KeyDbSizeCommand(dataStore));
        register(new KeyCommands.KeyFlushDbCommand(dataStore));
        register(new KeyCommands.KeyFlushAllCommand(dataStore));
        register(new KeyCommands.KeyPingCommand());
        register(new KeyCommands.KeyEchoCommand());
        register(new KeyCommands.KeySelectCommand());
        register(new KeyCommands.KeyInfoCommand());
        register(new KeyCommands.KeyTimeCommand());

        // List 命令
        register(new ListCommands.ListLPushCommand(dataStore));
        register(new ListCommands.ListRPushCommand(dataStore));
        register(new ListCommands.ListLPopCommand(dataStore));
        register(new ListCommands.ListRPopCommand(dataStore));
        register(new ListCommands.ListLLenCommand(dataStore));
        register(new ListCommands.ListLRangeCommand(dataStore));
        register(new ListCommands.ListLIndexCommand(dataStore));
        register(new ListCommands.ListLSetCommand(dataStore));
        register(new ListCommands.ListLTrimCommand(dataStore));

        // Hash 命令
        register(new HashCommands.HashHSetCommand(dataStore));
        register(new HashCommands.HashHSetNxCommand(dataStore));
        register(new HashCommands.HashHGetCommand(dataStore));
        register(new HashCommands.HashHGetAllCommand(dataStore));
        register(new HashCommands.HashHDelCommand(dataStore));
        register(new HashCommands.HashHExistsCommand(dataStore));
        register(new HashCommands.HashHLenCommand(dataStore));
        register(new HashCommands.HashHKeysCommand(dataStore));
        register(new HashCommands.HashHValsCommand(dataStore));
        register(new HashCommands.HashHMSetCommand(dataStore));
        register(new HashCommands.HashHMGetCommand(dataStore));
        register(new HashCommands.HashHIncrByCommand(dataStore));

        // Set 命令
        register(new SetCommands.SetSAddCommand(dataStore));
        register(new SetCommands.SetSRemCommand(dataStore));
        register(new SetCommands.SetSMembersCommand(dataStore));
        register(new SetCommands.SetSIsMemberCommand(dataStore));
        register(new SetCommands.SetSCardCommand(dataStore));
        register(new SetCommands.SetSInterCommand(dataStore));
        register(new SetCommands.SetSUnionCommand(dataStore));
        register(new SetCommands.SetSDiffCommand(dataStore));
        register(new SetCommands.SetSMoveCommand(dataStore));
        register(new SetCommands.SetSPopCommand(dataStore));
        register(new SetCommands.SetSRandMemberCommand(dataStore));

        logger.info("Registered {} commands", commands.size());
    }
}
