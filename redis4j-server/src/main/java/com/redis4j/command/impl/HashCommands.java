package com.redis4j.command.impl;

import com.redis4j.command.Command;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.storage.DataStore;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.*;

/**
 * Hash 命令实现
 */
public class HashCommands {

    // ==================== HSET ====================

    public static class HashHSetCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 3) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hset' command");
            }
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

    public static class HashHSetNxCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 3) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hsetnx' command");
            }
            boolean result = dataStore.hSetNx(args[0], args[1], args[2]);
            return RedisMessageHelper.integer(result ? 1 : 0);
        }
    }

    // ==================== HGET ====================

    public static class HashHGetCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hget' command");
            }
            String value = dataStore.hGet(args[0], args[1]);
            return RedisMessageHelper.bulkString(value);
        }
    }

    // ==================== HGETALL ====================

    public static class HashHGetAllCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hgetall' command");
            }
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

    public static class HashHDelCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hdel' command");
            }
            String key = args[0];
            String[] fields = Arrays.copyOfRange(args, 1, args.length);
            long count = dataStore.hDel(key, fields);
            return RedisMessageHelper.integer(count);
        }
    }

    // ==================== HEXISTS ====================

    public static class HashHExistsCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hexists' command");
            }
            boolean exists = dataStore.hExists(args[0], args[1]);
            return RedisMessageHelper.integer(exists ? 1 : 0);
        }
    }

    // ==================== HLEN ====================

    public static class HashHLenCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hlen' command");
            }
            long length = dataStore.hLen(args[0]);
            return RedisMessageHelper.integer(length);
        }
    }

    // ==================== HKEYS ====================

    public static class HashHKeysCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hkeys' command");
            }
            Set<String> keys = dataStore.hKeys(args[0]);
            return RedisMessageHelper.array(keys.stream()
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== HVALS ====================

    public static class HashHValsCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hvals' command");
            }
            String[] vals = dataStore.hVals(args[0]);
            return RedisMessageHelper.array(Arrays.stream(vals)
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== HMSET ====================

    public static class HashHMSetCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 3 || args.length % 2 != 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hmset' command");
            }
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

    public static class HashHMGetCommand implements Command {
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
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hmget' command");
            }
            String key = args[0];
            String[] fields = Arrays.copyOfRange(args, 1, args.length);
            String[] values = dataStore.hMGet(key, fields);
            return RedisMessageHelper.array(Arrays.stream(values)
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== HINCRBY ====================

    public static class HashHIncrByCommand implements Command {
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
        public RedisMessage execute(String[] args) {
            if (args.length < 3) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'hincrby' command");
            }
            long delta;
            try {
                delta = Long.parseLong(args[2]);
            } catch (NumberFormatException e) {
                return RedisMessageHelper.error("ERR", "value is not an integer or out of range");
            }
            try {
                long result = dataStore.hIncrBy(args[0], args[1], delta);
                return RedisMessageHelper.integer(result);
            } catch (Exception e) {
                return RedisMessageHelper.error("ERR", e.getMessage());
            }
        }
    }
}
