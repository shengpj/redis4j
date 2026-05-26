package com.redis4j.command.impl;

import com.redis4j.command.Command;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.storage.DataStore;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Arrays;

/**
 * List 命令实现
 */
public class ListCommands {

    // ==================== LPUSH ====================

    public static class ListLPushCommand implements Command {
        private final DataStore dataStore;

        public ListLPushCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LPUSH";
        }

        @Override
        public int getArity() {
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'lpush' command");
            }
            String key = args[0];
            String[] values = Arrays.copyOfRange(args, 1, args.length);
            long length = dataStore.lPush(key, values);
            return RedisMessageHelper.integer(length);
        }
    }

    // ==================== RPUSH ====================

    public static class ListRPushCommand implements Command {
        private final DataStore dataStore;

        public ListRPushCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "RPUSH";
        }

        @Override
        public int getArity() {
            return -1;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'rpush' command");
            }
            String key = args[0];
            String[] values = Arrays.copyOfRange(args, 1, args.length);
            long length = dataStore.rPush(key, values);
            return RedisMessageHelper.integer(length);
        }
    }

    // ==================== LPOP ====================

    public static class ListLPopCommand implements Command {
        private final DataStore dataStore;

        public ListLPopCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LPOP";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'lpop' command");
            }
            String value = dataStore.lPop(args[0]);
            return RedisMessageHelper.bulkString(value);
        }
    }

    // ==================== RPOP ====================

    public static class ListRPopCommand implements Command {
        private final DataStore dataStore;

        public ListRPopCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "RPOP";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'rpop' command");
            }
            String value = dataStore.rPop(args[0]);
            return RedisMessageHelper.bulkString(value);
        }
    }

    // ==================== LLEN ====================

    public static class ListLLenCommand implements Command {
        private final DataStore dataStore;

        public ListLLenCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LLEN";
        }

        @Override
        public int getArity() {
            return 2;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 1) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'llen' command");
            }
            long length = dataStore.lLen(args[0]);
            return RedisMessageHelper.integer(length);
        }
    }

    // ==================== LRANGE ====================

    public static class ListLRangeCommand implements Command {
        private final DataStore dataStore;

        public ListLRangeCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LRANGE";
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 3) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'lrange' command");
            }
            String key = args[0];
            long start, stop;
            try {
                start = Long.parseLong(args[1]);
                stop = Long.parseLong(args[2]);
            } catch (NumberFormatException e) {
                return RedisMessageHelper.error("ERR", "value is not an integer or out of range");
            }
            String[] values = dataStore.lRange(key, start, stop);
            return RedisMessageHelper.array(Arrays.stream(values)
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== LINDEX ====================

    public static class ListLIndexCommand implements Command {
        private final DataStore dataStore;

        public ListLIndexCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LINDEX";
        }

        @Override
        public int getArity() {
            return 3;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 2) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'lindex' command");
            }
            long index;
            try {
                index = Long.parseLong(args[1]);
            } catch (NumberFormatException e) {
                return RedisMessageHelper.error("ERR", "value is not an integer or out of range");
            }
            String value = dataStore.lIndex(args[0], index);
            return RedisMessageHelper.bulkString(value);
        }
    }

    // ==================== LSET ====================

    public static class ListLSetCommand implements Command {
        private final DataStore dataStore;

        public ListLSetCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LSET";
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 3) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'lset' command");
            }
            long index;
            try {
                index = Long.parseLong(args[1]);
            } catch (NumberFormatException e) {
                return RedisMessageHelper.error("ERR", "value is not an integer or out of range");
            }
            String value = args[2];
            dataStore.lSet(args[0], index, value);
            return RedisMessageHelper.ok();
        }
    }

    // ==================== LTRIM ====================

    public static class ListLTrimCommand implements Command {
        private final DataStore dataStore;

        public ListLTrimCommand(DataStore dataStore) {
            this.dataStore = dataStore;
        }

        @Override
        public String getName() {
            return "LTRIM";
        }

        @Override
        public int getArity() {
            return 4;
        }

        @Override
        public RedisMessage execute(String[] args) {
            if (args.length < 3) {
                return RedisMessageHelper.error("ERR", "wrong number of arguments for 'ltrim' command");
            }
            long start, stop;
            try {
                start = Long.parseLong(args[1]);
                stop = Long.parseLong(args[2]);
            } catch (NumberFormatException e) {
                return RedisMessageHelper.error("ERR", "value is not an integer or out of range");
            }
            dataStore.lTrim(args[0], start, stop);
            return RedisMessageHelper.ok();
        }
    }
}
