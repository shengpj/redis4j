package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.protocol.response.CommandResponses;
import com.redis4j.storage.HashStore;
import com.redis4j.protocol.response.CommandResponse;

import java.util.*;

/**
 * Hash 鍛戒护瀹炵幇
 */
public class HashCommands {

    // ==================== HSET ====================

    @RedisCommand
    public static class HashHSetCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHSetCommand(HashStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HSET";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 3 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 3 && args.length % 2 == 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            String key = args[0];
            long count = 0;
            for (int i = 1; i < args.length; i += 2) {
                if (i + 1 < args.length) {
                    long result = dataStore.hSet(key, args[i], args[i + 1]);
                    count += result;
                }
            }
            return CommandResponses.integer(count);
        }
    }

    // ==================== HSETNX ====================

    @RedisCommand
    public static class HashHSetNxCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHSetNxCommand(HashStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            boolean result = dataStore.hSetNx(args[0], args[1], args[2]);
            return CommandResponses.integer(result ? 1 : 0);
        }
    }

    // ==================== HGET ====================

    @RedisCommand
    public static class HashHGetCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHGetCommand(HashStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            String value = dataStore.hGet(args[0], args[1]);
            return CommandResponses.bulkString(value);
        }
    }

    // ==================== HGETALL ====================

    @RedisCommand
    public static class HashHGetAllCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHGetAllCommand(HashStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            Map<String, String> map = dataStore.hGetAll(args[0]);
            List<CommandResponse> result = new ArrayList<>();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                result.add(CommandResponses.bulkString(entry.getKey()));
                result.add(CommandResponses.bulkString(entry.getValue()));
            }
            return CommandResponses.array(result);
        }
    }

    // ==================== HDEL ====================

    @RedisCommand
    public static class HashHDelCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHDelCommand(HashStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HDEL";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 2 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 2;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            String key = args[0];
            String[] fields = Arrays.copyOfRange(args, 1, args.length);
            long count = dataStore.hDel(key, fields);
            return CommandResponses.integer(count);
        }
    }

    // ==================== HEXISTS ====================

    @RedisCommand
    public static class HashHExistsCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHExistsCommand(HashStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            boolean exists = dataStore.hExists(args[0], args[1]);
            return CommandResponses.integer(exists ? 1 : 0);
        }
    }

    // ==================== HLEN ====================

    @RedisCommand
    public static class HashHLenCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHLenCommand(HashStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long length = dataStore.hLen(args[0]);
            return CommandResponses.integer(length);
        }
    }

    // ==================== HKEYS ====================

    @RedisCommand
    public static class HashHKeysCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHKeysCommand(HashStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            Set<String> keys = dataStore.hKeys(args[0]);
            return CommandResponses.array(keys.stream()
                    .map(CommandResponses::bulkString)
                    .toList());
        }
    }

    // ==================== HVALS ====================

    @RedisCommand
    public static class HashHValsCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHValsCommand(HashStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            String[] vals = dataStore.hVals(args[0]);
            return CommandResponses.array(Arrays.stream(vals)
                    .map(CommandResponses::bulkString)
                    .toList());
        }
    }

    // ==================== HMSET ====================

    @RedisCommand
    public static class HashHMSetCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHMSetCommand(HashStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HMSET";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 3 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 3 && args.length % 2 == 1;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            String key = args[0];
            Map<String, String> map = new HashMap<>();
            for (int i = 1; i < args.length; i += 2) {
                map.put(args[i], args[i + 1]);
            }
            dataStore.hMSet(key, map);
            return CommandResponses.ok();
        }
    }

    // ==================== HMGET ====================

    @RedisCommand
    public static class HashHMGetCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHMGetCommand(HashStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "HMGET";
        }

        @Override
        public int getArity() {
            return -2; // 鑷冲皯 2 涓弬鏁?
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 2;
        }

        @Override
        protected CommandResponse doExecute(String[] args) {
            String key = args[0];
            String[] fields = Arrays.copyOfRange(args, 1, args.length);
            String[] values = dataStore.hMGet(key, fields);
            return CommandResponses.array(Arrays.stream(values)
                    .map(CommandResponses::bulkString)
                    .toList());
        }
    }

    // ==================== HINCRBY ====================

    @RedisCommand
    public static class HashHIncrByCommand extends AbstractCommand {
        private final HashStore dataStore;

        public HashHIncrByCommand(HashStore dataStore) {
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
        protected CommandResponse doExecute(String[] args) {
            long delta = parseLong(args[2]);
            long result = dataStore.hIncrBy(args[0], args[1], delta);
            return CommandResponses.integer(result);
        }
    }
}
