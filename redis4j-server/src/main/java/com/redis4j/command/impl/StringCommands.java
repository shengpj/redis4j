package com.redis4j.command.impl;

import com.redis4j.command.AbstractCommand;
import com.redis4j.command.annotation.RedisCommand;
import com.redis4j.protocol.RedisMessageHelper;
import com.redis4j.storage.DataStore;
import io.netty.handler.codec.redis.RedisMessage;

import java.util.Arrays;
import java.util.List;

/**
 * String 命令实现
 */
public class StringCommands {

    // ==================== GET ====================

    @RedisCommand(name = "GET", arity = 2)
    public static class StringGetCommand extends AbstractCommand {
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
        protected RedisMessage doExecute(String[] args) {
            String value = dataStore.get(args[0]);
            return RedisMessageHelper.bulkString(value);
        }
    }

    // ==================== SET ====================

    @RedisCommand(name = "SET", arity = -2)
    public static class StringSetCommand extends AbstractCommand {
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
            return -2; // 至少 2 个参数
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 2;
        }

        @Override
        protected String getValidationErrorMessage() {
            return "wrong number of arguments for 'set' command (at least 2 required)";
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String key = args[0];
            String value = args[1];
            dataStore.set(key, value);
            return RedisMessageHelper.ok();
        }
    }

    // ==================== SETNX ====================

    @RedisCommand(name = "SETNX", arity = 3)
    public static class StringSetNxCommand extends AbstractCommand {
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
        protected RedisMessage doExecute(String[] args) {
            boolean result = dataStore.setNx(args[0], args[1]);
            return RedisMessageHelper.integer(result ? 1 : 0);
        }
    }

    // ==================== SETEX ====================

    @RedisCommand(name = "SETEX", arity = 4)
    public static class StringSetExCommand extends AbstractCommand {
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
        protected RedisMessage doExecute(String[] args) {
            String key = args[0];
            long seconds = parseLong(args[1]);
            String value = args[2];
            dataStore.setEx(key, value, seconds);
            return RedisMessageHelper.ok();
        }

        @Override
        protected void logExecution(String[] args, RedisMessage result) {
            // SETEX 不需要详细日志
        }
    }

    // ==================== MGET ====================

    @RedisCommand(name = "MGET", arity = -2)
    public static class StringMGetCommand extends AbstractCommand {
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
            return -2;
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 1;
        }

        @Override
        protected String getValidationErrorMessage() {
            return "wrong number of arguments for 'mget' command (at least 1 required)";
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            String[] values = dataStore.mGet(args);
            return RedisMessageHelper.array(Arrays.stream(values)
                    .map(RedisMessageHelper::bulkString)
                    .toList());
        }
    }

    // ==================== MSET ====================

    @RedisCommand(name = "MSET", arity = -2)
    public static class StringMSetCommand extends AbstractCommand {
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
            return -2;
        }

        @Override
        protected boolean validate(String[] args) {
            return args != null && args.length >= 2 && args.length % 2 == 0;
        }

        @Override
        protected String getValidationErrorMessage() {
            return "wrong number of arguments for 'mset' command (even number of arguments required)";
        }

        @Override
        protected RedisMessage doExecute(String[] args) {
            java.util.Map<String, String> map = new java.util.HashMap<>();
            for (int i = 0; i < args.length; i += 2) {
                map.put(args[i], args[i + 1]);
            }
            dataStore.mSet(map);
            return RedisMessageHelper.ok();
        }
    }

    // ==================== INCR ====================

    @RedisCommand(name = "INCR", arity = 2)
    public static class StringIncrCommand extends AbstractCommand {
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
        protected RedisMessage doExecute(String[] args) {
            long result = dataStore.incr(args[0]);
            return RedisMessageHelper.integer(result);
        }
    }

    // ==================== INCRBY ====================

    @RedisCommand(name = "INCRBY", arity = 3)
    public static class StringIncrByCommand extends AbstractCommand {
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
        protected RedisMessage doExecute(String[] args) {
            long delta = parseLong(args[1]);
            long result = dataStore.incrBy(args[0], delta);
            return RedisMessageHelper.integer(result);
        }
    }

    // ==================== DECR ====================

    @RedisCommand(name = "DECR", arity = 2)
    public static class StringDecrCommand extends AbstractCommand {
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
        protected RedisMessage doExecute(String[] args) {
            long result = dataStore.decr(args[0]);
            return RedisMessageHelper.integer(result);
        }
    }

    // ==================== DECRBY ====================

    @RedisCommand(name = "DECRBY", arity = 3)
    public static class StringDecrByCommand extends AbstractCommand {
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
        protected RedisMessage doExecute(String[] args) {
            long delta = parseLong(args[1]);
            long result = dataStore.decrBy(args[0], delta);
            return RedisMessageHelper.integer(result);
        }
    }

    // ==================== STRLEN ====================

    @RedisCommand(name = "STRLEN", arity = 2)
    public static class StringStrlenCommand extends AbstractCommand {
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
        protected RedisMessage doExecute(String[] args) {
            long length = dataStore.strlen(args[0]);
            return RedisMessageHelper.integer(length);
        }
    }

    // ==================== APPEND ====================

    @RedisCommand(name = "APPEND", arity = 3)
    public static class StringAppendCommand extends AbstractCommand {
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
        protected RedisMessage doExecute(String[] args) {
            long length = dataStore.append(args[0], args[1]);
            return RedisMessageHelper.integer(length);
        }
    }
}
