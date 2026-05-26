package com.redis4j.command.impl;

import com.redis4j.command.Command;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.storage.DataStore;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Arrays;

/**
 * String 命令实现
 */
public class StringCommands {

    // ==================== GET ====================

    public static class StringGetCommand implements Command {
        private final DataStore dataStore;

        public StringGetCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "GET";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'get' command");
            }
            String value = dataStore.get(args[0]);
            return RedisMessageHelper.bulkString(value);
        }
    }

    // ==================== SET ====================

    public static class StringSetCommand implements Command {
        private final DataStore dataStore;

        public StringSetCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SET";
        }

        @Override
        public int getArity() {
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'set' command");
            }
            String key = args[0];
            String value = args[1];
            dataStore.set(key, value);
            return RedisMessageHelper.ok();
        }
    }

    // ==================== SETNX ====================

    public static class StringSetNxCommand implements Command {
        private final DataStore dataStore;

        public StringSetNxCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SETNX";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'setnx' command");
            }
            boolean result = dataStore.setNx(args[0], args[1]);
            return RedisMessageHelper.integer(result ? 1 : 0);
        }
    }

    // ==================== SETEX ====================

    public static class StringSetExCommand implements Command {
        private final DataStore dataStore;

        public StringSetExCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "SETEX";
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 3) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'setex' command");
            }
            String key = args[0];
            long seconds;
            try {
                seconds = Long.parseLong(args[1]);
            } catch (NumberFormatException e) {
                return RedisMessageHelper.error("ERR", "value is not an integer or out of range");
            }
            String value = args[2];
            dataStore.setEx(key, value, seconds);
            return RedisMessageHelper.ok();
        }
    }

    // ==================== MGET ====================

    public static class StringMGetCommand implements Command {
        private final DataStore dataStore;

        public StringMGetCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "MGET";
        }

        @Override
        public int getArity() {
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'mget' command");
            }
            String[] values = dataStore.mGet(args);
            return RedisMessageHelper.array(Arrays.stream(values)
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== MSET ====================

    public static class StringMSetCommand implements Command {
        private final DataStore dataStore;

        public StringMSetCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "MSET";
        }

        @Override
        public int getArity() {
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2 || args.length % 2 != 0) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'mset' command");
            }
            java.util.Map<String, String> map = new java.util.HashMap<>();
            for (int i = 0; i < args.length; i += 2) {
                map.put(args[i], args[i + 1]);
            }
            dataStore.mSet(map);
            return RedisMessageHelper.ok();
        }
    }

    // ==================== INCR ====================

    public static class StringIncrCommand implements Command {
        private final DataStore dataStore;

        public StringIncrCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "INCR";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'incr' command");
            }
            try {
                long result = dataStore.incr(args[0]);
                return RedisMessageHelper.integer(result);
            } catch (Exception e) {
                return RedisMessageHelper.error("ERR", e.getMessage());
            }
        }
    }

    // ==================== INCRBY ====================

    public static class StringIncrByCommand implements Command {
        private final DataStore dataStore;

        public StringIncrByCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "INCRBY";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'incrby' command");
            }
            long delta;
            try {
                delta = Long.parseLong(args[1]);
            } catch (NumberFormatException e) {
                return RedisMessageHelper.error("ERR", "value is not an integer or out of range");
            }
            try {
                long result = dataStore.incrBy(args[0], delta);
                return RedisMessageHelper.integer(result);
            } catch (Exception e) {
                return RedisMessageHelper.error("ERR", e.getMessage());
            }
        }
    }

    // ==================== DECR ====================

    public static class StringDecrCommand implements Command {
        private final DataStore dataStore;

        public StringDecrCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "DECR";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'decr' command");
            }
            try {
                long result = dataStore.decr(args[0]);
                return RedisMessageHelper.integer(result);
            } catch (Exception e) {
                return RedisMessageHelper.error("ERR", e.getMessage());
            }
        }
    }

    // ==================== DECRBY ====================

    public static class StringDecrByCommand implements Command {
        private final DataStore dataStore;

        public StringDecrByCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "DECRBY";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'decrby' command");
            }
            long delta;
            try {
                delta = Long.parseLong(args[1]);
            } catch (NumberFormatException e) {
                return RedisMessageHelper.error("ERR", "value is not an integer or out of range");
            }
            try {
                long result = dataStore.decrBy(args[0], delta);
                return RedisMessageHelper.integer(result);
            } catch (Exception e) {
                return RedisMessageHelper.error("ERR", e.getMessage());
            }
        }
    }

    // ==================== STRLEN ====================

    public static class StringStrlenCommand implements Command {
        private final DataStore dataStore;

        public StringStrlenCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "STRLEN";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'strlen' command");
            }
            long length = dataStore.strlen(args[0]);
            return RedisMessageHelper.integer(length);
        }
    }

    // ==================== APPEND ====================

    public static class StringAppendCommand implements Command {
        private final DataStore dataStore;

        public StringAppendCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "APPEND";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'append' command");
            }
            try {
                long length = dataStore.append(args[0], args[1]);
                return RedisMessageHelper.integer(length);
            } catch (Exception e) {
                return RedisMessageHelper.error("ERR", e.getMessage());
            }
        }
    }
}
