package com.redis4j.command.impl;

import com.redis4j.command.Command;
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

    public static class KeyDelCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'del' command");
            }
            long count = dataStore.del(args);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== EXISTS ====================

    public static class KeyExistsCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'exists' command");
            }
            long count = dataStore.exists(args);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== EXPIRE ====================

    public static class KeyExpireCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'expire' command");
            }
            long seconds;
            try {
                seconds = Long.parseLong(args[1]);
            } catch (NumberFormatException e) {
                return RedisMessageHelper.error("ERR", "value is not an integer or out of range");
            }
            boolean result = dataStore.expire(args[0], seconds);
            return RedisMessageHelper.integer(result ? 1 : 0);
        }
    }

    // ==================== EXPIREAT ====================

    public static class KeyExpireAtCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'expireat' command");
            }
            long timestamp;
            try {
                timestamp = Long.parseLong(args[1]);
            } catch (NumberFormatException e) {
                return RedisMessageHelper.error("ERR", "value is not an integer or out of range");
            }
            // 计算剩余秒数
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

    public static class KeyTtlCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'ttl' command");
            }
            long ttl = dataStore.ttl(args[0]);
            return RedisMessageHelper.integer(ttl);
        }
    }

    // ==================== PTTL ====================

    public static class KeyPttlCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'pttl' command");
            }
            long ttl = dataStore.pttl(args[0]);
            return RedisMessageHelper.integer(ttl);
        }
    }

    // ==================== PERSIST ====================

    public static class KeyPersistCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'persist' command");
            }
            boolean result = dataStore.persist(args[0]);
            return RedisMessageHelper.integer(result ? 1 : 0);
        }
    }

    // ==================== RENAME ====================

    public static class KeyRenameCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'rename' command");
            }
            dataStore.rename(args[0], args[1]);
            return RedisMessageHelper.ok();
        }
    }

    // ==================== TYPE ====================

    public static class KeyTypeCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'type' command");
            }
            String type = dataStore.type(args[0]).name().toLowerCase();
            return RedisMessageHelper.simpleString(type);
        }
    }

    // ==================== KEYS ====================

    public static class KeyKeysCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'keys' command");
            }
            Set<String> keys = dataStore.keys(args[0]);
            return RedisMessageHelper.array(keys.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== DBSIZE ====================

    public static class KeyDbSizeCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            long size = dataStore.dbSize();
            return RedisMessageHelper.integer(size);
        }
    }

    // ==================== FLUSHDB ====================

    public static class KeyFlushDbCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            dataStore.flushDb();
            return RedisMessageHelper.ok();
        }
    }

    // ==================== FLUSHALL ====================

    public static class KeyFlushAllCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            dataStore.flushAll();
            return RedisMessageHelper.ok();
        }
    }

    // ==================== PING ====================

    public static class KeyPingCommand implements Command {
        @Override
        public String getName() {
            return "PING";
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length > 0 && args[0] != null && !args[0].isEmpty()) {
                return RedisMessageHelper.bulkString(args[0]);
            }
            return RedisMessageHelper.pong();
        }
    }

    // ==================== ECHO ====================

    public static class KeyEchoCommand implements Command {
        @Override
        public String getName() {
            return "ECHO";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'echo' command");
            }
            return RedisMessageHelper.bulkString(args[0]);
        }
    }

    // ==================== SELECT ====================

    public static class KeySelectCommand implements Command {
        @Override
        public String getName() {
            return "SELECT";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public RedisMessage execute(String[] args) {
            // 当前只支持单数据库，返回 OK
            return RedisMessageHelper.ok();
        }
    }

    // ==================== INFO ====================

    public static class KeyInfoCommand implements Command {
        @Override
        public String getName() {
            return "INFO";
        }

        @Override
        public int getArity() {
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
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

    public static class KeyTimeCommand implements Command {
        @Override
        public String getName() {
            return "TIME";
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        public RedisMessage execute(String[] args) {
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
