package com.redis4j.command.impl;

import com.redis4j.command.CommandRegistry;
import com.redis4j.protocol.response.CommandResponse;
import com.redis4j.server.ServerConfig;
import com.redis4j.storage.MemoryStore;
import com.redis4j.storage.memory.EvictionPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerCommandsTest {

    @Test
    void infoMemoryReportsConfiguredLimitsAndCurrentEstimatedUsage() {
        try (MemoryStore store = new MemoryStore()) {
            store.set("key", "value");
            ServerConfig config = new ServerConfig();
            config.setMaxMemoryBytes(1024 * 1024);
            config.setMaxMemoryPolicy(EvictionPolicy.ALLKEYS_LRU);
            CommandRegistry registry = new CommandRegistry(store);
            registry.register(new ServerCommands.InfoCommand(store, config));

            CommandResponse response = registry.execute("INFO", new String[]{"memory"});

            CommandResponse.BulkString info = assertInstanceOf(CommandResponse.BulkString.class, response);
            assertTrue(info.value().contains("# Memory\r\n"));
            assertTrue(info.value().contains("used_memory:"));
            assertTrue(info.value().contains("used_memory_estimated:1\r\n"));
            assertTrue(info.value().contains("maxmemory:1048576\r\n"));
            assertTrue(info.value().contains("maxmemory_policy:allkeys-lru\r\n"));
            assertTrue(info.value().contains("db0_keys:1\r\n"));
        }
    }
}
