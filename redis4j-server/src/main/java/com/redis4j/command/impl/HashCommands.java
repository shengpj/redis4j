package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.storage.DataStore;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.*;

/**
 * Hash 命令实现
 */
public class HashCommands {

    // ==================== HSET ====================

    @RedisCommand(name = "HSET", arity = -2)
    public static class HashHSetCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHSetCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HSET";
        }

        @Override
        public int getArity() {
            return -2; // 至少 3 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 3 && args.length % 2 == 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String key = args[0];
            long count = 0;
            for (int i = 1; i < args.length; i += 2) {
                if (i + 1 < args.length) {
                    long result = dataStore.hSet(key, args[i], args[i + 1]);
                    count += result;
                }
            }
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== HSETNX ====================

    @RedisCommand(name = "HSETNX", arity = 4)
    public static class HashHSetNxCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHSetNxCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HSETNX";
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            boolean result = dataStore.hSetNx(args[0], args[1], args[2]);
            return RedisMessageHelper.integer(result ? 1 : 0);
        }
    }

    // ==================== HGET ====================

    @RedisCommand(name = "HGET", arity = 3)
    public static class HashHGetCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHGetCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HGET";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String value = dataStore.hGet(args[0], args[1]);
            return RedisMessageHelper.bulkString(value);
        }
    }

    // ==================== HGETALL ====================

    @RedisCommand(name = "HGETALL", arity = 2)
    public static class HashHGetAllCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHGetAllCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HGETALL";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            Map<String, String> map = dataStore.hGetAll(args[0]);
            List<RedisMessage> result = new ArrayList<>();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                result.add(RedisMessageHelper.bulkString(entry.getKey()));
                result.add(RedisMessageHelper.bulkString(entry.getValue()));
            }
            return RedisMessageHelper.array(result);
        }
    }

    // ==================== HDEL ====================

    @RedisCommand(name = "HDEL", arity = -2)
    public static class HashHDelCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHDelCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HDEL";
        }

        @Override
        public int getArity() {
            return -2; // 至少 2 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String key = args[0];
            String[] fields = Arrays.copyOfRange(args, 1, args.length);
            long count = dataStore.hDel(key, fields);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== HEXISTS ====================

    @RedisCommand(name = "HEXISTS", arity = 3)
    public static class HashHExistsCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHExistsCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HEXISTS";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            boolean exists = dataStore.hExists(args[0], args[1]);
            return RedisMessageHelper.integer(exists ? 1 : 0);
        }
    }

    // ==================== HLEN ====================

    @RedisCommand(name = "HLEN", arity = 2)
    public static class HashHLenCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHLenCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HLEN";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long length = dataStore.hLen(args[0]);
            return RedisMessageHelper.integer(length);
        }
    }

    // ==================== HKEYS ====================

    @RedisCommand(name = "HKEYS", arity = 2)
    public static class HashHKeysCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHKeysCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HKEYS";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            Set<String> keys = dataStore.hKeys(args[0]);
            return RedisMessageHelper.array(keys.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== HVALS ====================

    @RedisCommand(name = "HVALS", arity = 2)
    public static class HashHValsCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHValsCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HVALS";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String[] vals = dataStore.hVals(args[0]);
            return RedisMessageHelper.array(Arrays.stream(vals)
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== HMSET ====================

    @RedisCommand(name = "HMSET", arity = -2)
    public static class HashHMSetCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHMSetCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HMSET";
        }

        @Override
        public int getArity() {
            return -2; // 至少 3 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 3 && args.length % 2 == 1;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String key = args[0];
            Map<String, String> map = new HashMap<>();
            for (int i = 1; i < args.length; i += 2) {
                map.put(args[i], args[i + 1]);
            }
            dataStore.hMSet(key, map);
            return RedisMessageHelper.ok();
        }
    }

    // ==================== HMGET ====================

    @RedisCommand(name = "HMGET", arity = -2)
    public static class HashHMGetCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHMGetCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HMGET";
        }

        @Override
        public int getArity() {
            return -2; // 至少 2 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 2;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String key = args[0];
            String[] fields = Arrays.copyOfRange(args, 1, args.length);
            String[] values = dataStore.hMGet(key, fields);
            return RedisMessageHelper.array(Arrays.stream(values)
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== HINCRBY ====================

    @RedisCommand(name = "HINCRBY", arity = 4)
    public static class HashHIncrByCommand extends AbstractCommand {
        private final DataStore dataStore;

        public HashHIncrByCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HINCRBY";
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            long delta = parseLong(args[2]);
            long result = dataStore.hIncrBy(args[0], args[1], delta);
            return RedisMessageHelper.integer(result);
        }
    }
}
