package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.storage.DataStore;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Arrays;
import java.util.Set;

/**
 * Key 命令实现
 */
public class KeyCommands {

    // ==================== DEL ====================

    @RedisCommand(name = "DEL", arity = -2)
    public static class KeyDelCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyDelCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "DEL";
        }

        @Override
        public int getArity() {
            return -2; // 至少 1 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long count = dataStore.del(args);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== EXISTS ====================

    @RedisCommand(name = "EXISTS", arity = -2)
    public static class KeyExistsCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyExistsCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "EXISTS";
        }

        @Override
        public int getArity() {
            return -2; // 至少 1 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long count = dataStore.exists(args);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== EXPIRE ====================

    @RedisCommand(name = "EXPIRE", arity = 3)
    public static class KeyExpireCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyExpireCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "EXPIRE";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long seconds = Long.parseLong(args[1]);
            boolean result = dataStore.expire(args[0], seconds);
            return RedisMessageHelper.integer(result ? 1 : 0);
        }
    }

    // ==================== EXPIREAT ====================

    @RedisCommand(name = "EXPIREAT", arity = 3)
    public static class KeyExpireAtCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyExpireAtCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "EXPIREAT";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long timestamp = Long.parseLong(args[1]);
            long now = System.currentTimeMillis() / 1000;
            long seconds = timestamp - now;
            if (seconds <= 0) {
                dataStore.del(args[0]);
                return RedisMessageHelper.integer(1);
            }
            boolean result = dataStore.expire(args[0], seconds);
            return RedisMessageHelper.integer(result ? 1 : 0);
        }
    }

    // ==================== TTL ====================

    @RedisCommand(name = "TTL", arity = 2)
    public static class KeyTtlCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyTtlCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "TTL";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long ttl = dataStore.ttl(args[0]);
            return RedisMessageHelper.integer(ttl);
        }
    }

    // ==================== PTTL ====================

    @RedisCommand(name = "PTTL", arity = 2)
    public static class KeyPttlCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyPttlCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "PTTL";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long ttl = dataStore.pttl(args[0]);
            return RedisMessageHelper.integer(ttl);
        }
    }

    // ==================== PERSIST ====================

    @RedisCommand(name = "PERSIST", arity = 2)
    public static class KeyPersistCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyPersistCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "PERSIST";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            boolean result = dataStore.persist(args[0]);
            return RedisMessageHelper.integer(result ? 1 : 0);
        }
    }

    // ==================== RENAME ====================

    @RedisCommand(name = "RENAME", arity = 3)
    public static class KeyRenameCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyRenameCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "RENAME";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            dataStore.rename(args[0], args[1]);
            return RedisMessageHelper.ok();
        }
    }

    // ==================== TYPE ====================

    @RedisCommand(name = "TYPE", arity = 2)
    public static class KeyTypeCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyTypeCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "TYPE";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String type = dataStore.type(args[0]).name().toLowerCase();
            return RedisMessageHelper.simpleString(type);
        }
    }

    // ==================== KEYS ====================

    @RedisCommand(name = "KEYS", arity = 2)
    public static class KeyKeysCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyKeysCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "KEYS";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            Set<String> keys = dataStore.keys(args[0]);
            return RedisMessageHelper.array(keys.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== DBSIZE ====================

    @RedisCommand(name = "DBSIZE", arity = 1)
    public static class KeyDbSizeCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyDbSizeCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "DBSIZE";
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long size = dataStore.dbSize();
            return RedisMessageHelper.integer(size);
        }
    }

    // ==================== FLUSHDB ====================

    @RedisCommand(name = "FLUSHDB", arity = 1)
    public static class KeyFlushDbCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyFlushDbCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "FLUSHDB";
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            dataStore.flushDb();
            return RedisMessageHelper.ok();
        }
    }

    // ==================== FLUSHALL ====================

    @RedisCommand(name = "FLUSHALL", arity = 1)
    public static class KeyFlushAllCommand extends AbstractCommand {
        private final DataStore dataStore;

        public KeyFlushAllCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "FLUSHALL";
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            dataStore.flushAll();
            return RedisMessageHelper.ok();
        }
    }

    // ==================== PING ====================

    @RedisCommand(name = "PING", arity = 1)
    public static class KeyPingCommand extends AbstractCommand {
        @Override
        public String getName() {
            return "PING";
        }

        @Override
        public int getArity() {
            return 1; // 0 个参数
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            if (args.length > 0 && args[0] != null && !args[0].isEmpty()) {
                return RedisMessageHelper.bulkString(args[0]);
            }
            return RedisMessageHelper.pong();
        }
    }

    // ==================== ECHO ====================

    @RedisCommand(name = "ECHO", arity = 2)
    public static class KeyEchoCommand extends AbstractCommand {
        @Override
        public String getName() {
            return "ECHO";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            return RedisMessageHelper.bulkString(args[0]);
        }
    }

    // ==================== SELECT ====================

    @RedisCommand(name = "SELECT", arity = 2)
    public static class KeySelectCommand extends AbstractCommand {
        @Override
        public String getName() {
            return "SELECT";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            return RedisMessageHelper.ok();
        }
    }

    // ==================== INFO ====================

    @RedisCommand(name = "INFO", arity = 1)
    public static class KeyInfoCommand extends AbstractCommand {
        @Override
        public String getName() {
            return "INFO";
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            StringBuilder info = new StringBuilder();
            info.append("# Server\n");
            info.append("redis_version:1.0.0\n");
            info.append("redis4j_version:1.0.0\n");
            info.append("os:Java\n");
            info.append("# Stats\n");
            info.append("total_connections_received:1\n");
            return RedisMessageHelper.bulkString(info.toString());
        }
    }

    // ==================== TIME ====================

    @RedisCommand(name = "TIME", arity = 1)
    public static class KeyTimeCommand extends AbstractCommand {
        @Override
        public String getName() {
            return "TIME";
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long now = System.currentTimeMillis();
            long seconds = now / 1000;
            long microseconds = (now % 1000) * 1000;
            return RedisMessageHelper.array(Arrays.asList(
                    RedisMessageHelper.bulkString(String.valueOf(seconds)),
                    RedisMessageHelper.bulkString(String.valueOf(microseconds))
            ));
        }
    }
}
