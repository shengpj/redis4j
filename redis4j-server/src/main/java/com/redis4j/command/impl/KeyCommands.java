package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.storage.KeyStore;
import com.redis4j.protocol.response.CommandResponse;

import java.util.Arrays;
import java.util.Set;

/**
 * Key 鍛戒护瀹炵幇
 */
public class KeyCommands {

    // ==================== DEL ====================

    @RedisCommand
    public static class KeyDelCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyDelCommand(KeyStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "DEL";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 1 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            long count = dataStore.del(args);
            return CommandResponses.integer(count);
        }
    }

    // ==================== EXISTS ====================

    @RedisCommand
    public static class KeyExistsCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyExistsCommand(KeyStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "EXISTS";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 1 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            long count = dataStore.exists(args);
            return CommandResponses.integer(count);
        }
    }

    // ==================== EXPIRE ====================

    @RedisCommand
    public static class KeyExpireCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyExpireCommand(KeyStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long seconds = parseLong(args[1]);
            boolean result = dataStore.expire(args[0], seconds);
            return CommandResponses.integer(result ? 1 : 0);
        }
    }

    // ==================== EXPIREAT ====================

    @RedisCommand
    public static class KeyExpireAtCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyExpireAtCommand(KeyStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long timestamp = parseLong(args[1]);
            long now = System.currentTimeMillis() / 1000;
            long seconds = timestamp - now;
            if (seconds <= 0) {
                dataStore.del(args[0]);
                return CommandResponses.integer(1);
            }
            boolean result = dataStore.expire(args[0], seconds);
            return CommandResponses.integer(result ? 1 : 0);
        }
    }

    // ==================== PEXPIREAT ====================

    @RedisCommand
    public static class KeyPExpireAtCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyPExpireAtCommand(KeyStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "PEXPIREAT";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            long timestamp = parseLong(args[1]);
            long milliseconds = timestamp - System.currentTimeMillis();
            if (milliseconds <= 0) {
                return CommandResponses.integer(dataStore.del(args[0]));
            }
            return CommandResponses.integer(dataStore.expireMs(args[0], milliseconds) ? 1 : 0);
        }
    }

    // ==================== TTL ====================

    @RedisCommand
    public static class KeyTtlCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyTtlCommand(KeyStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long ttl = dataStore.ttl(args[0]);
            return CommandResponses.integer(ttl);
        }
    }

    // ==================== PTTL ====================

    @RedisCommand
    public static class KeyPttlCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyPttlCommand(KeyStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long ttl = dataStore.pttl(args[0]);
            return CommandResponses.integer(ttl);
        }
    }

    // ==================== PERSIST ====================

    @RedisCommand
    public static class KeyPersistCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyPersistCommand(KeyStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            boolean result = dataStore.persist(args[0]);
            return CommandResponses.integer(result ? 1 : 0);
        }
    }

    // ==================== RENAME ====================

    @RedisCommand
    public static class KeyRenameCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyRenameCommand(KeyStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            dataStore.rename(args[0], args[1]);
            return CommandResponses.ok();
        }
    }

    // ==================== TYPE ====================

    @RedisCommand
    public static class KeyTypeCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyTypeCommand(KeyStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            String type = dataStore.type(args[0]).name().toLowerCase();
            return CommandResponses.simpleString(type);
        }
    }

    // ==================== KEYS ====================

    @RedisCommand
    public static class KeyKeysCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyKeysCommand(KeyStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            Set<String> keys = dataStore.keys(args[0]);
            return CommandResponses.array(keys.stream()
                    .map(CommandResponses::bulkString)
                    .toList());
        }
    }

    // ==================== DBSIZE ====================

    @RedisCommand
    public static class KeyDbSizeCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyDbSizeCommand(KeyStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long size = dataStore.dbSize();
            return CommandResponses.integer(size);
        }
    }

    // ==================== FLUSHDB ====================

    @RedisCommand
    public static class KeyFlushDbCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyFlushDbCommand(KeyStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            dataStore.flushDb();
            return CommandResponses.ok();
        }
    }

    // ==================== FLUSHALL ====================

    @RedisCommand
    public static class KeyFlushAllCommand extends AbstractCommand {
        private final KeyStore dataStore;

        public KeyFlushAllCommand(KeyStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            dataStore.flushAll();
            return CommandResponses.ok();
        }
    }

    // ==================== PING ====================

    @RedisCommand
    public static class KeyPingCommand extends AbstractCommand {
        @Override
        public String getName() {
            return "PING";
        }

        @Override
        public int getArity() {
            return 1; // 0 涓弬鏁?
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            if (args.length > 0 && args[0] != null && !args[0].isEmpty()) {
                return CommandResponses.bulkString(args[0]);
            }
            return CommandResponses.pong();
        }
    }

    // ==================== ECHO ====================

    @RedisCommand
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
        protected CommandResponse doExecute(String[] args) {
            return CommandResponses.bulkString(args[0]);
        }
    }

    // ==================== SELECT ====================

    @RedisCommand
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
        protected CommandResponse doExecute(String[] args) {
            return CommandResponses.ok();
        }
    }

    // ==================== INFO ====================

    @RedisCommand
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
        protected CommandResponse doExecute(String[] args) {
            StringBuilder info = new StringBuilder();
            info.append("# Server\n");
            info.append("redis_version:1.0.0\n");
            info.append("redis4j_version:1.0.0\n");
            info.append("os:Java\n");
            info.append("# Stats\n");
            info.append("total_connections_received:1\n");
            return CommandResponses.bulkString(info.toString());
        }
    }

    // ==================== TIME ====================

    @RedisCommand
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
        protected CommandResponse doExecute(String[] args) {
            long now = System.currentTimeMillis();
            long seconds = now / 1000;
            long microseconds = (now % 1000) * 1000;
            return CommandResponses.array(Arrays.asList(
                    CommandResponses.bulkString(String.valueOf(seconds)),
                    CommandResponses.bulkString(String.valueOf(microseconds))
            ));
        }
    }
}
